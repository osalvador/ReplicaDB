package org.replicadb.server.job.persistence;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStateMachine;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.application.RunRecoveryResult;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class JobRunRepository implements JobRunStore {

    private static final String SELECT_COLUMNS = """
            id, job_definition_id, previous_run_id, status, attempt, executor_identity,
            lease_until, heartbeat_at, created_at, started_at, finished_at,
            rows_processed, duration_millis, committed_watermark, error_message, cancellation_warning,
            available_at, lease_token
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final JobRunRowMapper rowMapper;

    public JobRunRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.rowMapper = new JobRunRowMapper();
    }

        @Override
        public JobRun insertPendingNow(UUID jobDefinitionId, UUID previousRunId, int attempt) {
            return insertPendingNow(UUID.randomUUID(), jobDefinitionId, previousRunId, attempt);
        }

        @Override
        public JobRun insertPendingNow(UUID runId, UUID jobDefinitionId, UUID previousRunId, int attempt) {
            return insertPendingWithDatabaseTime(runId, jobDefinitionId, previousRunId, attempt);
        }

        private JobRun insertPendingWithDatabaseTime(UUID id, UUID jobDefinitionId,
                                                     UUID previousRunId, int attempt) {
        String sql = """
            INSERT INTO job_run (id, job_definition_id, previous_run_id, status, attempt,
                         available_at, created_at)
            VALUES (:id, :jobDefinitionId, :previousRunId, 'PENDING', :attempt,
                now(), now())
            """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("jobDefinitionId", jobDefinitionId)
            .addValue("previousRunId", previousRunId, Types.OTHER)
            .addValue("attempt", attempt);
        try {
            jdbcTemplate.update(sql, parameters);
        } catch (DuplicateKeyException exception) {
            throw new IllegalStateException(
                "Job definition " + jobDefinitionId + " already has an active run", exception);
        }
        return findById(id).orElseThrow(() -> new IllegalStateException(
            "Created JobRun could not be loaded: " + id));
        }

    @Transactional
    public Optional<JobRun> claimNextEligible(UUID requestedRunId, String executorIdentity,
                                              Duration leaseDuration) {
        if (leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }

        String selectSql;
        MapSqlParameterSource selectParameters = new MapSqlParameterSource();
        if (requestedRunId == null) {
            selectSql = """
                    SELECT id FROM job_run
                    WHERE status = 'PENDING' AND available_at <= now()
                    ORDER BY available_at, created_at, id
                    LIMIT 1 FOR UPDATE SKIP LOCKED
                    """;
        } else {
            selectSql = """
                    SELECT id FROM job_run
                    WHERE id = :id AND status = 'PENDING' AND available_at <= now()
                    FOR UPDATE SKIP LOCKED
                    """;
            selectParameters.addValue("id", requestedRunId);
        }
        List<UUID> ids = jdbcTemplate.query(selectSql, selectParameters,
                (resultSet, rowNum) -> resultSet.getObject("id", UUID.class));
        if (ids.isEmpty()) {
            return Optional.empty();
        }
        UUID runId = ids.get(0);
        LeaseToken leaseToken = LeaseToken.generate();

        String updateSql = """
                UPDATE job_run
                SET status = 'RUNNING', executor_identity = :executorIdentity,
                    lease_token = :leaseToken,
                    lease_until = now() + (:leaseSeconds * interval '1 second'),
                    started_at = now(), heartbeat_at = now()
                WHERE id = :id AND status = 'PENDING' AND available_at <= now()
                """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("executorIdentity", executorIdentity)
                .addValue("leaseSeconds", Math.max(1, leaseDuration.toSeconds()))
                .addValue("id", runId)
                .addValue("leaseToken", leaseToken.value(), Types.OTHER);
        if (jdbcTemplate.update(updateSql, parameters) != 1) {
            throw new IllegalStateException("Could not claim pending JobRun " + runId);
        }

        return findById(runId);
    }

    @Transactional
    public JobRunStore.LeaseRenewalResult renewLease(UUID runId, LeaseToken leaseToken,
                                                     Duration leaseDuration) {
        if (runId == null) {
            throw new IllegalArgumentException("runId must not be null");
        }
        if (leaseToken == null) {
            throw new IllegalArgumentException("leaseToken must not be null");
        }
        if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }

        String sql = """
                UPDATE job_run
                SET lease_until = now() + (:leaseSeconds * interval '1 second'),
                    heartbeat_at = now()
                WHERE id = :id AND lease_token = :leaseToken
                  AND status IN ('RUNNING', 'CANCEL_REQUESTED')
                  AND lease_until > now()
                """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("leaseToken", leaseToken.value(), Types.OTHER)
                .addValue("leaseSeconds", Math.max(1, leaseDuration.toSeconds()));
        if (jdbcTemplate.update(sql, parameters) == 1) {
            return JobRunStore.LeaseRenewalResult.RENEWED;
        }

        Boolean exists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM job_run WHERE id = :id)",
                Map.of("id", runId), Boolean.class);
        return Boolean.TRUE.equals(exists)
                ? JobRunStore.LeaseRenewalResult.FENCED
                : JobRunStore.LeaseRenewalResult.NOT_FOUND;
    }

    @Transactional
    public RunRecoveryResult recoverExpiredRun(UUID runId) {
        if (runId == null) {
            throw new IllegalArgumentException("runId must not be null");
        }

        String selectSql = """
                SELECT r.id, r.job_definition_id, r.previous_run_id, r.status, r.attempt,
                       r.executor_identity, r.lease_until, r.heartbeat_at, r.created_at,
                       r.started_at, r.finished_at, r.rows_processed, r.duration_millis,
                       r.committed_watermark, r.error_message, r.cancellation_warning,
                       r.available_at, r.lease_token,
                       d.max_attempts, d.retry_backoff_seconds, d.automatic_retry_enabled
                FROM job_run r
                JOIN job_definition d ON d.id = r.job_definition_id
                WHERE r.id = :id
                  AND r.status IN ('RUNNING', 'CANCEL_REQUESTED')
                  AND r.lease_until <= now()
                FOR UPDATE OF r, d SKIP LOCKED
                """;
        List<RecoveryCandidate> candidates = jdbcTemplate.query(selectSql, Map.of("id", runId),
                (resultSet, rowNum) -> new RecoveryCandidate(
                        rowMapper.mapRow(resultSet, rowNum),
                        new RetryPolicy(resultSet.getInt("max_attempts"),
                                resultSet.getLong("retry_backoff_seconds"),
                                resultSet.getBoolean("automatic_retry_enabled"))));
        if (candidates.isEmpty()) {
            return new RunRecoveryResult(Optional.empty(), Optional.empty());
        }

        RecoveryCandidate candidate = candidates.get(0);
        JobRun abandoned = candidate.run();
        if (abandoned.status() == JobRunStatus.CANCEL_REQUESTED) {
            JobRunStateMachine.assertLegalTransition(JobRunStatus.CANCEL_REQUESTED, JobRunStatus.CANCELLED);
            int updated = jdbcTemplate.update("""
                    UPDATE job_run
                    SET status = 'CANCELLED', finished_at = now(),
                        rows_processed = COALESCE(rows_processed, 0),
                        duration_millis = COALESCE(duration_millis, 0),
                        error_message = NULL
                    WHERE id = :id AND status = 'CANCEL_REQUESTED' AND lease_until <= now()
                    """, Map.of("id", runId));
            assertUpdated(runId, updated, JobRunStatus.CANCELLED);
            return new RunRecoveryResult(Optional.of(findById(runId).orElseThrow()), Optional.empty());
        }

        boolean canRetry = candidate.retryPolicy().automaticRetryEnabled()
                && abandoned.attempt() < candidate.retryPolicy().maxAttempts();
        if (canRetry) {
            JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.RETRY_SCHEDULED);
            int updated = jdbcTemplate.update("""
                    UPDATE job_run
                    SET status = 'RETRY_SCHEDULED', finished_at = now()
                    WHERE id = :id AND status = 'RUNNING' AND lease_until <= now()
                    """, Map.of("id", runId));
            assertUpdated(runId, updated, JobRunStatus.RETRY_SCHEDULED);

            UUID replacementId = UUID.randomUUID();
            String insertSql = """
                    INSERT INTO job_run (
                        id, job_definition_id, previous_run_id, status, attempt, available_at, created_at
                    ) VALUES (
                        :id, :jobDefinitionId, :previousRunId, 'PENDING', :attempt,
                        now() + (:retryBackoffSeconds * interval '1 second'), now()
                    )
                    """;
            jdbcTemplate.update(insertSql, new MapSqlParameterSource()
                    .addValue("id", replacementId)
                    .addValue("jobDefinitionId", abandoned.jobDefinitionId())
                    .addValue("previousRunId", abandoned.id(), Types.OTHER)
                    .addValue("attempt", abandoned.attempt() + 1)
                    .addValue("retryBackoffSeconds", candidate.retryPolicy().retryBackoffSeconds()));
            return new RunRecoveryResult(
                    Optional.of(findById(runId).orElseThrow()),
                    Optional.of(findById(replacementId).orElseThrow()));
        }

        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.FAILED);
        int updated = jdbcTemplate.update("""
                UPDATE job_run
                SET status = 'FAILED', finished_at = now(),
                    error_message = 'Lease expired before execution completed'
                WHERE id = :id AND status = 'RUNNING' AND lease_until <= now()
                """, Map.of("id", runId));
        assertUpdated(runId, updated, JobRunStatus.FAILED);
        return new RunRecoveryResult(Optional.of(findById(runId).orElseThrow()), Optional.empty());
    }

    @Override
    public List<UUID> findExpiredRunIds(int limit) {
        validateLimit(limit);
        return jdbcTemplate.query("""
                SELECT id
                FROM job_run
                WHERE status IN ('RUNNING', 'CANCEL_REQUESTED')
                  AND lease_until IS NOT NULL
                  AND lease_until <= now()
                ORDER BY lease_until, created_at, id
                LIMIT :limit
                """, Map.of("limit", limit),
                (resultSet, rowNum) -> resultSet.getObject("id", UUID.class));
    }

    @Override
    public List<UUID> findCancellationRequestedRunIds(String executorIdentity, int limit) {
        if (executorIdentity == null || executorIdentity.isBlank()) {
            throw new IllegalArgumentException("executorIdentity must not be blank");
        }
        validateLimit(limit);
        return jdbcTemplate.query("""
                SELECT id
                FROM job_run
                WHERE status = 'CANCEL_REQUESTED' AND executor_identity = :executorIdentity
                ORDER BY created_at, id
                LIMIT :limit
                """, Map.of("executorIdentity", executorIdentity, "limit", limit),
                (resultSet, rowNum) -> resultSet.getObject("id", UUID.class));
    }

            @Override
            public JobRunStore.EligibleRunSnapshot findEligibleRunSnapshot(int limit) {
            validateLimit(limit);
            List<Instant> availableAt = jdbcTemplate.query("""
                SELECT available_at
                FROM job_run
                WHERE status = 'PENDING' AND available_at <= now()
                ORDER BY available_at, created_at, id
                LIMIT :limit
                """, Map.of("limit", limit + 1),
                (resultSet, rowNum) -> resultSet.getTimestamp("available_at").toInstant());
            boolean truncated = availableAt.size() > limit;
            List<Instant> bounded = truncated ? availableAt.subList(0, limit) : availableAt;
            return new JobRunStore.EligibleRunSnapshot(bounded.size(), truncated,
                bounded.isEmpty() ? null : bounded.get(0));
            }

    public boolean hasActiveRun(UUID jobDefinitionId) {
        String sql = """
                SELECT EXISTS(
                    SELECT 1 FROM job_run
                    WHERE job_definition_id = :jobDefinitionId
                      AND status IN ('PENDING', 'RUNNING', 'CANCEL_REQUESTED')
                )
                """;
        Boolean active = jdbcTemplate.queryForObject(sql,
                Map.of("jobDefinitionId", jobDefinitionId), Boolean.class);
        return Boolean.TRUE.equals(active);
    }

        public JobRunStore.FencedUpdateResult recordProgress(UUID runId, LeaseToken leaseToken,
                                  long rowsProcessed, long durationMillis) {
        validateFencedValues(runId, leaseToken, rowsProcessed, durationMillis);
        String sql = """
            UPDATE job_run
            SET rows_processed = :rowsProcessed, duration_millis = :durationMillis
            WHERE id = :id AND lease_token = :leaseToken
              AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """;
        int updated = jdbcTemplate.update(sql, fencedParameters(runId, leaseToken)
            .addValue("rowsProcessed", rowsProcessed)
            .addValue("durationMillis", durationMillis));
        return fencedResult(runId, updated);
        }

        public JobRunStore.FencedUpdateResult markSucceeded(UUID runId, LeaseToken leaseToken,
                                long rowsProcessed, long durationMillis,
                                String committedWatermark) {
        validateFencedValues(runId, leaseToken, rowsProcessed, durationMillis);
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.SUCCEEDED);
        String sql = """
            UPDATE job_run
            SET status = 'SUCCEEDED', finished_at = now(), rows_processed = :rowsProcessed,
                duration_millis = :durationMillis, committed_watermark = :committedWatermark,
                error_message = NULL
            WHERE id = :id AND lease_token = :leaseToken AND status = 'RUNNING'
            """;
        int updated = jdbcTemplate.update(sql, fencedParameters(runId, leaseToken)
            .addValue("rowsProcessed", rowsProcessed)
            .addValue("durationMillis", durationMillis)
            .addValue("committedWatermark", committedWatermark, Types.VARCHAR));
        return fencedResult(runId, updated);
        }

    public JobRunStore.FencedUpdateResult markFailed(UUID runId, LeaseToken leaseToken,
                                                     long rowsProcessed, long durationMillis,
                                                     String errorMessage) {
        validateFencedValues(runId, leaseToken, rowsProcessed, durationMillis);
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.FAILED);
        String sql = """
                UPDATE job_run
                SET status = 'FAILED', finished_at = now(), rows_processed = :rowsProcessed,
                    duration_millis = :durationMillis, error_message = :errorMessage
                WHERE id = :id AND lease_token = :leaseToken AND status = 'RUNNING'
                """;
        int updated = jdbcTemplate.update(sql, fencedParameters(runId, leaseToken)
                .addValue("rowsProcessed", rowsProcessed)
                .addValue("durationMillis", durationMillis)
                .addValue("errorMessage", errorMessage, Types.VARCHAR));
        return fencedResult(runId, updated);
    }

    @Override
    public JobRunStore.CancellationResult requestCancellation(UUID runId, String cancellationWarning) {
        if (runId == null) {
            throw new IllegalArgumentException("runId must not be null");
        }
        String sql = """
                UPDATE job_run
                SET status = 'CANCEL_REQUESTED', cancellation_warning = :cancellationWarning
                WHERE id = :id AND status = 'RUNNING'
                """;
        int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("cancellationWarning", cancellationWarning, Types.VARCHAR));
        if (updated == 1) {
            return JobRunStore.CancellationResult.REQUESTED;
        }
        return cancellationStatus(runId);
    }

    @Override
    public JobRunStore.CancellationResult cancelPending(UUID runId, String cancellationWarning) {
        if (runId == null) {
            throw new IllegalArgumentException("runId must not be null");
        }
        String sql = """
                UPDATE job_run
                SET status = 'CANCELLED', finished_at = now(), rows_processed = 0,
                    duration_millis = 0, error_message = NULL,
                    cancellation_warning = :cancellationWarning
                WHERE id = :id AND status = 'PENDING'
                """;
        int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("cancellationWarning", cancellationWarning, Types.VARCHAR));
        if (updated == 1) {
            return JobRunStore.CancellationResult.CANCELLED;
        }
        return cancellationStatus(runId);
    }

    public JobRunStore.FencedUpdateResult markCancelled(UUID runId, LeaseToken leaseToken,
                                                        long rowsProcessed, long durationMillis) {
        validateFencedValues(runId, leaseToken, rowsProcessed, durationMillis);
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.CANCELLED);
        JobRunStateMachine.assertLegalTransition(JobRunStatus.CANCEL_REQUESTED, JobRunStatus.CANCELLED);
        String sql = """
                UPDATE job_run
                SET status = 'CANCELLED', finished_at = now(), rows_processed = :rowsProcessed,
                    duration_millis = :durationMillis, error_message = NULL
                WHERE id = :id AND lease_token = :leaseToken
                  AND status IN ('RUNNING', 'CANCEL_REQUESTED')
                """;
        int updated = jdbcTemplate.update(sql, fencedParameters(runId, leaseToken)
                .addValue("rowsProcessed", rowsProcessed)
                .addValue("durationMillis", durationMillis));
        return fencedResult(runId, updated);
    }

    @Transactional
    @Override
    public JobRun scheduleRetryNow(UUID failedRunId) {
        JobRun failedRun = findById(failedRunId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown JobRun " + failedRunId));
        if (failedRun.status() != JobRunStatus.FAILED) {
            throw new IllegalStateException("Only failed JobRuns can be retried: " + failedRunId);
        }
        JobRunStateMachine.assertLegalTransition(JobRunStatus.FAILED, JobRunStatus.RETRY_SCHEDULED);

        int updated = jdbcTemplate.update("""
                UPDATE job_run
                SET status = 'RETRY_SCHEDULED'
                WHERE id = :id AND status = 'FAILED'
                """, Map.of("id", failedRunId));
        assertUpdated(failedRunId, updated, JobRunStatus.RETRY_SCHEDULED);

        return insertPendingWithDatabaseTime(failedRun.jobDefinitionId(), failedRun.id(),
                failedRun.attempt() + 1);
    }

    private JobRun insertPendingWithDatabaseTime(UUID jobDefinitionId, UUID previousRunId, int attempt) {
        return insertPendingNow(jobDefinitionId, previousRunId, attempt);
    }

    private static void validateLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
    }

    public Optional<String> findLastCommittedWatermark(UUID jobDefinitionId) {
        String sql = """
                SELECT committed_watermark
                FROM job_run
                WHERE job_definition_id = :jobDefinitionId AND status = 'SUCCEEDED'
                ORDER BY finished_at DESC NULLS LAST, created_at DESC
                LIMIT 1
                """;
        return jdbcTemplate.query(sql, Map.of("jobDefinitionId", jobDefinitionId),
                (resultSet, rowNum) -> resultSet.getString("committed_watermark"))
                .stream()
                .findFirst();
    }

    public Optional<JobRun> findById(UUID id) {
        String sql = "SELECT " + SELECT_COLUMNS + " FROM job_run WHERE id = :id";
        return jdbcTemplate.query(sql, Map.of("id", id), rowMapper).stream().findFirst();
    }

    public List<JobRun> findPage(UUID jobDefinitionId, JobRunStatus status, int page, int size,
                                 Set<UUID> restrictToJobIds) {
        validatePage(page, size);
        StringBuilder sql = new StringBuilder("SELECT " + SELECT_COLUMNS + " FROM job_run WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, jobDefinitionId, status, restrictToJobIds);
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :size OFFSET :offset");
        parameters.addValue("size", size).addValue("offset", (long) page * size);
        return jdbcTemplate.query(sql.toString(), parameters, rowMapper);
    }

    public long count(UUID jobDefinitionId, JobRunStatus status, Set<UUID> restrictToJobIds) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM job_run WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, jobDefinitionId, status, restrictToJobIds);
        Long count = jdbcTemplate.queryForObject(sql.toString(), parameters, Long.class);
        return count == null ? 0 : count;
    }

    private static void appendFilters(StringBuilder sql, MapSqlParameterSource parameters,
                                      UUID jobDefinitionId, JobRunStatus status,
                                      Set<UUID> restrictToJobIds) {
        if (jobDefinitionId != null) {
            sql.append(" AND job_definition_id = :jobDefinitionId");
            parameters.addValue("jobDefinitionId", jobDefinitionId);
        }
        if (status != null) {
            sql.append(" AND status = :status");
            parameters.addValue("status", status.name());
        }
        if (restrictToJobIds != null) {
            sql.append(" AND job_definition_id = ANY(:restrictToJobIds)");
            parameters.addValue("restrictToJobIds", restrictToJobIds.toArray(UUID[]::new), Types.ARRAY);
        }
    }

    private static void validatePage(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must not be negative");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size must be positive");
        }
    }

    private static void assertUpdated(UUID runId, int updated, JobRunStatus targetStatus) {
        if (updated != 1) {
            throw new IllegalStateException("Could not transition JobRun " + runId + " to " + targetStatus);
        }
    }

    private static void validateFencedValues(UUID runId, LeaseToken leaseToken,
                                             long rowsProcessed, long durationMillis) {
        if (runId == null) {
            throw new IllegalArgumentException("runId must not be null");
        }
        if (leaseToken == null) {
            throw new IllegalArgumentException("leaseToken must not be null");
        }
        if (rowsProcessed < 0) {
            throw new IllegalArgumentException("rowsProcessed must not be negative");
        }
        if (durationMillis < 0) {
            throw new IllegalArgumentException("durationMillis must not be negative");
        }
    }

    private MapSqlParameterSource fencedParameters(UUID runId, LeaseToken leaseToken) {
        return new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("leaseToken", leaseToken.value(), Types.OTHER);
    }

    private JobRunStore.FencedUpdateResult fencedResult(UUID runId, int updated) {
        if (updated == 1) {
            return JobRunStore.FencedUpdateResult.UPDATED;
        }
        Boolean exists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM job_run WHERE id = :id)",
                Map.of("id", runId), Boolean.class);
        return Boolean.TRUE.equals(exists)
                ? JobRunStore.FencedUpdateResult.FENCED
                : JobRunStore.FencedUpdateResult.NOT_FOUND;
    }

    private JobRunStore.CancellationResult cancellationStatus(UUID runId) {
        Optional<JobRunStatus> status = jdbcTemplate.query("SELECT status FROM job_run WHERE id = :id",
                Map.of("id", runId), (resultSet, rowNum) -> JobRunStatus.valueOf(resultSet.getString("status")))
                .stream().findFirst();
        if (status.isEmpty()) {
            return JobRunStore.CancellationResult.NOT_FOUND;
        }
        return switch (status.get()) {
            case CANCEL_REQUESTED -> JobRunStore.CancellationResult.ALREADY_REQUESTED;
            case PENDING, RUNNING -> JobRunStore.CancellationResult.TERMINAL;
            default -> JobRunStore.CancellationResult.TERMINAL;
        };
    }

    private record RecoveryCandidate(JobRun run, RetryPolicy retryPolicy) {
    }

}
