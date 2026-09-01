package org.replicadb.server.job.application;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.persistence.PostgresNotificationPublisher;
import org.replicadb.server.job.persistence.RunTriggerIdempotencyRepository;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class RunDispatchServiceIT {

    @Autowired
    private RunDispatchService dispatchService;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private RunTriggerIdempotencyRepository idempotencyRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @SpyBean
    private PostgresNotificationPublisher notificationPublisher;

    @BeforeEach
    void clearState() {
        reset(notificationPublisher);
        jdbcTemplate.update("TRUNCATE TABLE run_trigger_idempotency, job_run, job_definition, "
            + "datasource_permission, managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @Test
    void commitsRunIdempotencyAndNotificationTogether() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());

        try (Connection listener = listeningConnection(RunNotificationPublisher.RUN_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            RunDispatchResult result = dispatchService.dispatchManual(definition.id(), "atomic-key");

            JobRun run = result.run().orElseThrow();
            assertTrue(result.created());
            assertEquals(run.id(), idempotencyRepository.findValidRunId("atomic-key").orElseThrow());
            List<PGNotification> notifications = notifications(pgConnection, 2_000);
                assertEquals(1, notifications.stream()
                    .filter(notification -> run.id().toString().equals(notification.getParameter()))
                    .count());
        }
    }

    @Test
    void notificationFailureRollsBackRunAndIdempotency() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        doThrow(new IllegalStateException("notification failed"))
                .when(notificationPublisher).publishRun(any(UUID.class));

        assertThrows(IllegalStateException.class,
                () -> dispatchService.dispatchManual(definition.id(), "rollback-key"));

        assertTrue(idempotencyRepository.findValidRunId("rollback-key").isEmpty());
        assertEquals(0, jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM job_run WHERE job_definition_id = :jobDefinitionId
                """, Map.of("jobDefinitionId", definition.id()), Integer.class));
    }

    @Test
    void concurrentSameKeyRequestsReturnOneCreatedRunAndOneReplay() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<RunDispatchResult> first = executor.submit(
                    () -> dispatchAfter(start, definition.id(), "same-key"));
            Future<RunDispatchResult> second = executor.submit(
                    () -> dispatchAfter(start, definition.id(), "same-key"));
            start.countDown();

            RunDispatchResult firstResult = first.get(10, TimeUnit.SECONDS);
            RunDispatchResult secondResult = second.get(10, TimeUnit.SECONDS);
            assertEquals(1, (firstResult.created() ? 1 : 0) + (secondResult.created() ? 1 : 0));
            assertEquals(1, (firstResult.replayed() ? 1 : 0) + (secondResult.replayed() ? 1 : 0));
            assertEquals(firstResult.run().orElseThrow().id(), secondResult.run().orElseThrow().id());
            assertEquals(1, jdbcTemplate.queryForObject("""
                    SELECT COUNT(*) FROM job_run WHERE job_definition_id = :jobDefinitionId
                    """, Map.of("jobDefinitionId", definition.id()), Integer.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentDifferentKeysKeepTheActiveRunConflictClean() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<RunDispatchResult> first = executor.submit(
                    () -> dispatchAfter(start, definition.id(), "active-key-one"));
            Future<RunDispatchResult> second = executor.submit(
                    () -> dispatchAfter(start, definition.id(), "active-key-two"));
            start.countDown();

            int successful = 0;
            int conflicts = 0;
            for (Future<RunDispatchResult> future : List.of(first, second)) {
                try {
                    future.get(10, TimeUnit.SECONDS);
                    successful++;
                } catch (ExecutionException exception) {
                    assertTrue(exception.getCause() instanceof IllegalStateException);
                    conflicts++;
                }
            }
            assertEquals(1, successful);
            assertEquals(1, conflicts);
            assertEquals(1, jdbcTemplate.queryForObject("""
                    SELECT COUNT(*) FROM job_run WHERE job_definition_id = :jobDefinitionId
                    """, Map.of("jobDefinitionId", definition.id()), Integer.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void retryPublishesOnlyTheNewAttemptAndRecoveryPublishesReplacement() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definitionWithRetry());
        JobRun failed = jobRunRepository.claimNextEligible(
                jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
                "failed-worker", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markFailed(failed.id(), failed.leaseToken(), 0, 0, "temporary");

        RunDispatchResult retry;
        try (Connection listener = listeningConnection(RunNotificationPublisher.RUN_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            retry = dispatchService.dispatchRetry(failed.id());
            assertEquals(JobRunStatus.PENDING, retry.run().orElseThrow().status());
            assertEquals(failed.id(), retry.run().orElseThrow().previousRunId());
            assertEquals(1, notifications(pgConnection, 2_000).size());
        }

        JobRun running = jobRunRepository.claimNextEligible(
                retry.run().orElseThrow().id(), "expired-worker", Duration.ofMinutes(5)).orElseThrow();
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
                Map.of("id", running.id()));
        try (Connection listener = listeningConnection(RunNotificationPublisher.RUN_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            RunDispatchResult recovery = dispatchService.recoverExpiredRun(running.id());
            assertTrue(recovery.replacementCreated());
            assertEquals(1, notifications(pgConnection, 2_000).size());
        }
    }

        @Test
        void rejectsManualAndScheduledDispatchWhenEitherBindingIsDisabled() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        jdbcTemplate.update("""
            UPDATE job_definition
            SET source_datasource_use_enabled = false
            WHERE id = :id
            """, Map.of("id", definition.id()));

        assertThrows(IllegalStateException.class,
            () -> dispatchService.dispatchManual(definition.id(), "disabled-manual"));
        assertThrows(IllegalStateException.class,
            () -> dispatchService.dispatchScheduled(definition.id()));

        assertEquals(0, jdbcTemplate.queryForObject("""
            SELECT COUNT(*) FROM job_run WHERE job_definition_id = :jobDefinitionId
            """, Map.of("jobDefinitionId", definition.id()), Integer.class));
        assertTrue(idempotencyRepository.findValidRunId("disabled-manual").isEmpty());
        }

        @Test
        void keepsRetryPendingUntilBindingsAreReenabled() {
        JobDefinition definition = jobDefinitionRepository.insert(definitionWithRetry());
        JobRun failed = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
            "failed-worker", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markFailed(failed.id(), failed.leaseToken(), 0, 0, "temporary");
        disableBindings(definition.id());

        JobRun replacement = dispatchService.dispatchRetry(failed.id()).run().orElseThrow();

        assertEquals(JobRunStatus.PENDING, replacement.status());
        assertTrue(jobRunRepository.claimAndPrepare(replacement.id(), "disabled-worker",
            Duration.ofMinutes(5)).isEmpty());

        enableBindings(definition.id());
        assertTrue(jobRunRepository.claimAndPrepare(replacement.id(), "enabled-worker",
            Duration.ofMinutes(5)).isPresent());
        }

        @Test
        void keepsRecoveryReplacementPendingUntilBindingsAreReenabled() {
        JobDefinition definition = jobDefinitionRepository.insert(definitionWithRetry());
        JobRun claimed = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
            "expired-worker", Duration.ofMinutes(5)).orElseThrow();
        disableBindings(definition.id());
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
            Map.of("id", claimed.id()));

        JobRun replacement = dispatchService.recoverExpiredRun(claimed.id()).run().orElseThrow();

        assertEquals(JobRunStatus.PENDING, replacement.status());
        assertTrue(jobRunRepository.claimAndPrepare(replacement.id(), "disabled-worker",
            Duration.ofMinutes(5)).isEmpty());

        enableBindings(definition.id());
        assertTrue(jobRunRepository.claimAndPrepare(replacement.id(), "enabled-worker",
            Duration.ofMinutes(5)).isPresent());
        }

    private RunDispatchResult dispatchAfter(CountDownLatch start, UUID jobDefinitionId, String key)
            throws InterruptedException {
        start.await(5, TimeUnit.SECONDS);
        return dispatchService.dispatchManual(jobDefinitionId, key);
    }

    private Connection listeningConnection(String... channels) throws Exception {
        Connection connection = dataSource.getConnection();
        try (Statement statement = connection.createStatement()) {
            for (String channel : channels) {
                statement.execute("LISTEN " + channel);
            }
        }
        return connection;
    }

    private static List<PGNotification> notifications(PGConnection connection, int timeoutMillis) throws Exception {
        PGNotification[] notifications = connection.getNotifications(timeoutMillis);
        return notifications == null ? List.of() : Arrays.asList(notifications);
    }

    private static JobDefinition definition() {
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("dispatch-job-" + UUID.randomUUID())
                .withDefaultDatasourceReferences()
                .build();
    }

    private static JobDefinition definitionWithRetry() {
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("retry-job-" + UUID.randomUUID())
            .withDefaultDatasourceReferences()
                .withMode(ReplicationMode.INCREMENTAL)
                .withIncrementalWatermarkColumn("updated_at")
                .withRetryPolicy(new RetryPolicy(3, 0, true))
                .build();
    }

    private void disableBindings(UUID jobDefinitionId) {
        jdbcTemplate.update("""
                UPDATE job_definition
                SET source_datasource_use_enabled = false,
                    sink_datasource_use_enabled = false
                WHERE id = :id
                """, Map.of("id", jobDefinitionId));
    }

    private void enableBindings(UUID jobDefinitionId) {
        jdbcTemplate.update("""
                UPDATE job_definition
                SET source_datasource_use_enabled = true,
                    sink_datasource_use_enabled = true
                WHERE id = :id
                """, Map.of("id", jobDefinitionId));
    }
}
