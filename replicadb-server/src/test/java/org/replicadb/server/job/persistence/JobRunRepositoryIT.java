package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobRunRepositoryIT {

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void claimsPendingRunAndSetsRunningFields() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);

        JobRun claimed = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertEquals(pending.id(), claimed.id());
        assertEquals(JobRunStatus.RUNNING, claimed.status());
        assertEquals("worker-1", claimed.executorIdentity());
        assertTrue(claimed.startedAt() != null);
        assertTrue(claimed.heartbeatAt() != null);
        assertTrue(claimed.leaseUntil() != null);
    }

    @Test
    void skipsALockedPendingRowAndClaimsTheNextOne() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun second = jobRunRepository.insertPending(definition.id(), null, 1);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            UUID lockedId;
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT id FROM job_run ORDER BY created_at, id LIMIT 1 FOR UPDATE")) {
                assertTrue(resultSet.next());
                lockedId = (UUID) resultSet.getObject("id");
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<JobRun> claimedFuture = executor.submit(
                        () -> jobRunRepository.claimNextPending("worker-2", Duration.ofMinutes(5))
                                .orElseThrow());
                JobRun claimed = claimedFuture.get(2, TimeUnit.SECONDS);

                assertEquals(second.id(), claimed.id());
                assertEquals(JobRunStatus.RUNNING, claimed.status());
                assertNotEquals(first.id(), claimed.id());
                assertNotEquals(lockedId, claimed.id());
            } finally {
                executor.shutdownNow();
            }
            connection.commit();
        }
    }

    @Test
    void returnsEmptyWhenNoPendingRunExists() {
        assertTrue(jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).isEmpty());
    }

    @Test
    void rejectsIllegalTransitionAfterCancellation() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();

        jobRunRepository.markCancelled(running.id(), 4, 12);

        assertThrows(IllegalStateException.class,
                () -> jobRunRepository.markSucceeded(pending.id(), 4, 12, "42"));
    }

    @Test
    void findsLastCommittedWatermarkOnlyFromSuccessfulRuns() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        jobRunRepository.insertPending(definition.id(), null, 1);

        assertTrue(jobRunRepository.findLastCommittedWatermark(definition.id()).isEmpty());

        JobRun running = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(running.id(), 4, 12, "42");

        assertEquals("42", jobRunRepository.findLastCommittedWatermark(definition.id()).orElseThrow());
    }

    @Test
    void schedulesRetryAsANewPendingRun() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun failed = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markFailed(failed.id(), 4, 12, "temporary failure");

        JobRun retry = jobRunRepository.scheduleRetry(failed.id());

        assertEquals(JobRunStatus.RETRY_SCHEDULED,
                jobRunRepository.findById(failed.id()).orElseThrow().status());
        assertEquals(JobRunStatus.PENDING, retry.status());
        assertEquals(failed.id(), retry.previousRunId());
        assertEquals(2, retry.attempt());
    }

    @Test
    void rejectsRetryForNonFailedRuns() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());

        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        assertThrows(IllegalStateException.class, () -> jobRunRepository.scheduleRetry(pending.id()));

        JobRun running = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();
        assertThrows(IllegalStateException.class, () -> jobRunRepository.scheduleRetry(running.id()));

        jobRunRepository.markSucceeded(running.id(), 4, 12, "42");
        assertThrows(IllegalStateException.class, () -> jobRunRepository.scheduleRetry(running.id()));
    }

    private static JobDefinition definition() {
        return new JobDefinition(
                null, "job-" + UUID.randomUUID(), "jdbc:source", null, "${env:SOURCE_PASSWORD}",
                "source_table", null, "jdbc:sink", null, "${env:SINK_PASSWORD}", "sink_table",
                ReplicationMode.INCREMENTAL, 1, "updated_at", "0", null, null);
    }
}