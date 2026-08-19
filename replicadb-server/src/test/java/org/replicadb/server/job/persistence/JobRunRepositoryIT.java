package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        JobDefinition firstDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondDefinition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPending(firstDefinition.id(), null, 1);
        JobRun second = jobRunRepository.insertPending(secondDefinition.id(), null, 1);

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
        JobDefinition pendingDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition runningDefinition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(pendingDefinition.id(), null, 1);
        jobRunRepository.insertPending(runningDefinition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();

        jobRunRepository.markCancelled(running.id(), 4, 12);

        assertThrows(IllegalStateException.class,
                () -> jobRunRepository.markSucceeded(pending.id(), 4, 12, "42"));
    }

    @Test
    void claimsOnlyTheRequestedPendingRun() {
        JobDefinition firstDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondDefinition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPending(firstDefinition.id(), null, 1);
        JobRun second = jobRunRepository.insertPending(secondDefinition.id(), null, 1);

        JobRun claimed = jobRunRepository.claimById(second.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertEquals(second.id(), claimed.id());
        assertEquals(JobRunStatus.PENDING,
                jobRunRepository.findById(first.id()).orElseThrow().status());
        assertEquals(JobRunStatus.RUNNING, claimed.status());
    }

    @Test
    void returnsEmptyWhenRequestedRunIsNotPendingOrDoesNotExist() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimById(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertTrue(jobRunRepository.claimById(running.id(), "worker-2", Duration.ofMinutes(5)).isEmpty());
        assertTrue(jobRunRepository.claimById(UUID.randomUUID(), "worker-2", Duration.ofMinutes(5)).isEmpty());
    }

    @Test
    void reportsOnlyActiveStatuses() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        assertTrue(jobRunRepository.hasActiveRun(definition.id()));

        JobRun running = jobRunRepository.claimById(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();
        assertTrue(jobRunRepository.hasActiveRun(definition.id()));
        jobRunRepository.markCancelRequested(running.id(), "cancel warning");
        assertTrue(jobRunRepository.hasActiveRun(definition.id()));
        jobRunRepository.markCancelled(running.id(), 0, 0);
        assertTrue(!jobRunRepository.hasActiveRun(definition.id()));
    }

    @Test
    void rejectsConcurrentPendingRunsForOneDefinitionButAllowsAnotherAfterTerminalState() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var start = new java.util.concurrent.CountDownLatch(1);
            Future<?> first = executor.submit(() -> insertAfter(start, definition.id()));
            Future<?> second = executor.submit(() -> insertAfter(start, definition.id()));
            start.countDown();

            int successes = 0;
            int failures = 0;
            for (Future<?> future : new Future<?>[]{first, second}) {
                try {
                    future.get(2, TimeUnit.SECONDS);
                    successes++;
                } catch (java.util.concurrent.ExecutionException exception) {
                    assertTrue(exception.getCause() instanceof IllegalStateException);
                    failures++;
                }
            }
            assertEquals(1, successes);
            assertEquals(1, failures);

            JobRun running = jobRunRepository.claimNextPending("worker-1", Duration.ofMinutes(5)).orElseThrow();
            jobRunRepository.markSucceeded(running.id(), 0, 0, null);
            assertTrue(!jobRunRepository.hasActiveRun(definition.id()));
            jobRunRepository.insertPending(definition.id(), null, 2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void markCancelRequestedIsIdempotentAfterTerminalTransition() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimById(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(running.id(), 0, 0, null);

        jobRunRepository.markCancelRequested(running.id(), "ignored warning");
        JobRun unchanged = jobRunRepository.findById(running.id()).orElseThrow();
        assertEquals(JobRunStatus.SUCCEEDED, unchanged.status());
        assertNull(unchanged.cancellationWarning());
    }

    @Test
    void cancelsAPendingRunWithoutClaimingIt() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);

        jobRunRepository.markPendingCancelled(pending.id(), "pending warning");

        JobRun cancelled = jobRunRepository.findById(pending.id()).orElseThrow();
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
        assertEquals(0, cancelled.rowsProcessed());
        assertEquals("pending warning", cancelled.cancellationWarning());
    }

    @Test
    void preservesCancellationWarningWhenExecutorFinishesCancellation() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimById(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();

        jobRunRepository.markCancelRequested(running.id(), "indeterminate sink warning");
        assertEquals("indeterminate sink warning",
                jobRunRepository.findById(running.id()).orElseThrow().cancellationWarning());

        jobRunRepository.markCancelled(running.id(), 0, 0);

        JobRun cancelled = jobRunRepository.findById(running.id()).orElseThrow();
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
        assertEquals("indeterminate sink warning", cancelled.cancellationWarning());
    }

    @Test
    void leavesCancellationWarningNullForUncancelledRun() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);

        assertNull(jobRunRepository.findById(pending.id()).orElseThrow().cancellationWarning());
    }

    @Test
    void paginatesRunsAndFiltersByDefinitionAndStatus() {
        UUID filteredDefinitionId = null;
        for (int index = 0; index < 5; index++) {
            JobDefinition definition = jobDefinitionRepository.insert(definition());
            if (index == 0) {
                filteredDefinitionId = definition.id();
            }
            JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
            JobRun running = jobRunRepository.claimById(pending.id(), "worker-" + index,
                    Duration.ofMinutes(5)).orElseThrow();
            if (index % 2 == 0) {
                jobRunRepository.markSucceeded(running.id(), index, index, null);
            } else {
                jobRunRepository.markFailed(running.id(), index, index, "failure");
            }
        }

        assertEquals(5, jobRunRepository.count(null, null, null));
        assertEquals(2, jobRunRepository.findPage(null, JobRunStatus.FAILED, 0, 2, null).size());
        assertEquals(3, jobRunRepository.count(null, JobRunStatus.SUCCEEDED, null));
        assertEquals(1, jobRunRepository.count(filteredDefinitionId, null, null));

        java.util.List<JobRun> firstPage = jobRunRepository.findPage(null, null, 0, 2, null);
        java.util.List<JobRun> secondPage = jobRunRepository.findPage(null, null, 1, 2, null);
        java.util.List<JobRun> thirdPage = jobRunRepository.findPage(null, null, 2, 2, null);
        assertEquals(2, firstPage.size());
        assertEquals(2, secondPage.size());
        assertEquals(1, thirdPage.size());
        assertEquals(5, java.util.stream.Stream.of(firstPage, secondPage, thirdPage)
                .mapToInt(java.util.List::size).sum());

        assertEquals(1, jobRunRepository.count(null, null, Set.of(filteredDefinitionId)));
        assertEquals(1, jobRunRepository.findPage(null, null, 0, 10, Set.of(filteredDefinitionId)).size());
        assertEquals(0, jobRunRepository.count(null, null, Set.of()));
        assertTrue(jobRunRepository.findPage(null, null, 0, 10, Set.of()).isEmpty());
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
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName("job-" + UUID.randomUUID())
            .withSourcePassword("${env:SOURCE_PASSWORD}")
            .withSinkPassword("${env:SINK_PASSWORD}")
            .withMode(ReplicationMode.INCREMENTAL)
            .withIncrementalWatermarkColumn("updated_at")
            .withInitialWatermarkValue("0")
            .build();
    }

    private void insertAfter(java.util.concurrent.CountDownLatch start, UUID definitionId) {
        try {
            start.await(2, TimeUnit.SECONDS);
            jobRunRepository.insertPending(definitionId, null, 1);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }
}
