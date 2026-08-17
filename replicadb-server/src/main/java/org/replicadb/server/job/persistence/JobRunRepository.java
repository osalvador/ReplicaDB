package org.replicadb.server.job.persistence;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStateMachine;
import org.replicadb.server.job.domain.JobRunStatus;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class JobRunRepository {

    private static final String SELECT_COLUMNS = """
            id, job_definition_id, previous_run_id, status, attempt, executor_identity,
            lease_until, heartbeat_at, created_at, started_at, finished_at,
            rows_processed, duration_millis, committed_watermark, error_message, cancellation_warning
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JobRunRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JobRun insertPending(UUID jobDefinitionId, UUID previousRunId, int attempt) {
        UUID id = UUID.randomUUID();
        Instant createdAt = Instant.now();
        String sql = """
                INSERT INTO job_run (id, job_definition_id, previous_run_id, status, attempt, created_at)
                VALUES (:id, :jobDefinitionId, :previousRunId, 'PENDING', :attempt, :createdAt)
                """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("jobDefinitionId", jobDefinitionId)
                .addValue("previousRunId", previousRunId, Types.OTHER)
                .addValue("attempt", attempt)
                .addValue("createdAt", Timestamp.from(createdAt));
        try {
            jdbcTemplate.update(sql, parameters);
        } catch (DuplicateKeyException exception) {
            throw new IllegalStateException(
                    "Job definition " + jobDefinitionId + " already has an active run", exception);
        }
        return new JobRun(id, jobDefinitionId, previousRunId, JobRunStatus.PENDING, attempt,
            null, null, null, createdAt, null, null, null, null, null, null, null);
    }

    @Transactional
    public Optional<JobRun> claimById(UUID runId, String executorIdentity, Duration leaseDuration) {
        if (leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }

        List<UUID> ids = jdbcTemplate.getJdbcTemplate().query(
                "SELECT id FROM job_run WHERE id = ? AND status = 'PENDING' FOR UPDATE SKIP LOCKED",
                (resultSet, rowNum) -> resultSet.getObject("id", UUID.class), runId);
        if (ids.isEmpty()) {
            return Optional.empty();
        }

        String updateSql = """
                UPDATE job_run
                SET status = 'RUNNING', executor_identity = :executorIdentity,
                    lease_until = now() + (:leaseSeconds * interval '1 second'),
                    started_at = now(), heartbeat_at = now()
                WHERE id = :id AND status = 'PENDING'
                """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("executorIdentity", executorIdentity)
                .addValue("leaseSeconds", Math.max(1, leaseDuration.toSeconds()))
                .addValue("id", runId);
        if (jdbcTemplate.update(updateSql, parameters) != 1) {
            throw new IllegalStateException("Could not claim pending JobRun " + runId);
        }

        return findById(runId);
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

    @Transactional
    public Optional<JobRun> claimNextPending(String executorIdentity, Duration leaseDuration) {
        if (leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }

        String selectSql = "SELECT id FROM job_run WHERE status = 'PENDING' "
                + "ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED";
        List<UUID> ids = jdbcTemplate.getJdbcTemplate().query(
                selectSql,
                (resultSet, rowNum) -> resultSet.getObject("id", UUID.class));
        if (ids.isEmpty()) {
            return Optional.empty();
        }

        UUID id = ids.get(0);
        String updateSql = """
                UPDATE job_run
                SET status = 'RUNNING', executor_identity = :executorIdentity,
                    lease_until = now() + (:leaseSeconds * interval '1 second'),
                    started_at = now(), heartbeat_at = now()
                WHERE id = :id AND status = 'PENDING'
                """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("executorIdentity", executorIdentity)
                .addValue("leaseSeconds", Math.max(1, leaseDuration.toSeconds()))
                .addValue("id", id);
        if (jdbcTemplate.update(updateSql, parameters) != 1) {
            throw new IllegalStateException("Could not claim pending JobRun " + id);
        }

        return findById(id);
    }

    public void markSucceeded(UUID runId, long rowsProcessed, long durationMillis, String committedWatermark) {
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.SUCCEEDED);
        String sql = """
                UPDATE job_run
                SET status = 'SUCCEEDED', finished_at = now(), rows_processed = :rowsProcessed,
                    duration_millis = :durationMillis, committed_watermark = :committedWatermark,
                    error_message = NULL
                WHERE id = :id AND status = 'RUNNING'
                """;
        int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("rowsProcessed", rowsProcessed)
                .addValue("durationMillis", durationMillis)
                .addValue("committedWatermark", committedWatermark, Types.VARCHAR));
        assertUpdated(runId, updated, JobRunStatus.SUCCEEDED);
    }

    public void markFailed(UUID runId, long rowsProcessed, long durationMillis, String errorMessage) {
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.FAILED);
        String sql = """
                UPDATE job_run
                SET status = 'FAILED', finished_at = now(), rows_processed = :rowsProcessed,
                    duration_millis = :durationMillis, error_message = :errorMessage
                WHERE id = :id AND status = 'RUNNING'
                """;
        int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("rowsProcessed", rowsProcessed)
                .addValue("durationMillis", durationMillis)
                .addValue("errorMessage", errorMessage, Types.VARCHAR));
        assertUpdated(runId, updated, JobRunStatus.FAILED);
    }

    public void markCancelRequested(UUID runId, String cancellationWarning) {
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.CANCEL_REQUESTED);
        String sql = """
                UPDATE job_run
                SET status = 'CANCEL_REQUESTED', cancellation_warning = :cancellationWarning
                WHERE id = :id AND status = 'RUNNING'
                """;
            int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("cancellationWarning", cancellationWarning, Types.VARCHAR));
        if (updated == 0) {
            // The execution may have reached a terminal state between the API read and this update.
            return;
        }
        assertUpdated(runId, updated, JobRunStatus.CANCEL_REQUESTED);
    }

    public void markPendingCancelled(UUID runId, String cancellationWarning) {
        JobRunStateMachine.assertLegalTransition(JobRunStatus.PENDING, JobRunStatus.CANCELLED);
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
        assertUpdated(runId, updated, JobRunStatus.CANCELLED);
    }

    public void markCancelled(UUID runId, long rowsProcessed, long durationMillis) {
        JobRunStateMachine.assertLegalTransition(JobRunStatus.RUNNING, JobRunStatus.CANCELLED);
        JobRunStateMachine.assertLegalTransition(JobRunStatus.CANCEL_REQUESTED, JobRunStatus.CANCELLED);
        String sql = """
                UPDATE job_run
                SET status = 'CANCELLED', finished_at = now(), rows_processed = :rowsProcessed,
                    duration_millis = :durationMillis, error_message = NULL
                WHERE id = :id AND status IN ('RUNNING', 'CANCEL_REQUESTED')
                """;
        int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("rowsProcessed", rowsProcessed)
                .addValue("durationMillis", durationMillis));
        assertUpdated(runId, updated, JobRunStatus.CANCELLED);
    }

    @Transactional
    public JobRun scheduleRetry(UUID failedRunId) {
        JobRun failedRun = findById(failedRunId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown JobRun " + failedRunId));
        if (failedRun.status() != JobRunStatus.FAILED) {
            throw new IllegalStateException("Only failed JobRuns can be retried: " + failedRunId);
        }
        JobRunStateMachine.assertLegalTransition(JobRunStatus.FAILED, JobRunStatus.RETRY_SCHEDULED);

        String sql = """
                UPDATE job_run
                SET status = 'RETRY_SCHEDULED'
                WHERE id = :id AND status = 'FAILED'
                """;
        int updated = jdbcTemplate.update(sql, Map.of("id", failedRunId));
        assertUpdated(failedRunId, updated, JobRunStatus.RETRY_SCHEDULED);

        return insertPending(failedRun.jobDefinitionId(), failedRun.id(), failedRun.attempt() + 1);
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
        return jdbcTemplate.query(sql, Map.of("id", id), ROW_MAPPER).stream().findFirst();
    }

    public List<JobRun> findPage(UUID jobDefinitionId, JobRunStatus status, int page, int size,
                                 Set<UUID> restrictToJobIds) {
        validatePage(page, size);
        StringBuilder sql = new StringBuilder("SELECT " + SELECT_COLUMNS + " FROM job_run WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, jobDefinitionId, status, restrictToJobIds);
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :size OFFSET :offset");
        parameters.addValue("size", size).addValue("offset", (long) page * size);
        return jdbcTemplate.query(sql.toString(), parameters, ROW_MAPPER);
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

    private static final RowMapper<JobRun> ROW_MAPPER = new RowMapper<>() {
        @Override
        public JobRun mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            return new JobRun(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getObject("job_definition_id", UUID.class),
                    resultSet.getObject("previous_run_id", UUID.class),
                    JobRunStatus.valueOf(resultSet.getString("status")),
                    resultSet.getInt("attempt"),
                    resultSet.getString("executor_identity"),
                    toInstant(resultSet.getTimestamp("lease_until")),
                    toInstant(resultSet.getTimestamp("heartbeat_at")),
                    toInstant(resultSet.getTimestamp("created_at")),
                    toInstant(resultSet.getTimestamp("started_at")),
                    toInstant(resultSet.getTimestamp("finished_at")),
                    nullableLong(resultSet, "rows_processed"),
                    nullableLong(resultSet, "duration_millis"),
                    resultSet.getString("committed_watermark"),
                    resultSet.getString("error_message"),
                    resultSet.getString("cancellation_warning"));
        }
    };

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Long nullableLong(ResultSet resultSet, String column) throws SQLException {
        long value = resultSet.getLong(column);
        return resultSet.wasNull() ? null : value;
    }
}
