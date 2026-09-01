package org.replicadb.server.job.dispatch;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.ReplicaDbServerApplication;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.application.RunDispatchResult;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.application.RunFinalizationService;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.domain.ManagedDataSourceTestSupport;
import org.replicadb.server.job.domain.TestKeyring;
import org.replicadb.server.job.execution.RunExecutionCoordinator;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.persistence.PostgresNotificationPublisher;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.session.SessionRepository;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DistributedWorkerLifecycleIT {

    private static final String API_PROFILE = "api";
    private static final String FIRST_WORKER = "distributed-worker-one";
    private static final String SECOND_WORKER = "distributed-worker-two";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    private static String schema;
    private static String metadataUrl;
    private static ConfigurableApplicationContext apiContext;
    private static ConfigurableApplicationContext firstWorkerContext;
    private static ConfigurableApplicationContext secondWorkerContext;
    private static Path keyringPath;

    @TempDir
    Path tempDirectory;

    @BeforeAll
    static void startContexts() throws Exception {
        schema = PostgresTestcontainersConfig.isolatedSchema();
        PostgresTestcontainersConfig.migrate(POSTGRES, schema);
        metadataUrl = PostgresTestcontainersConfig.jdbcUrl(POSTGRES, schema);
        keyringPath = TestKeyring.create();
        apiContext = application(API_PROFILE, null);
        firstWorkerContext = application("worker", FIRST_WORKER);
        secondWorkerContext = application("worker", SECOND_WORKER);
    }

    @AfterAll
    static void stopContexts() throws Exception {
        close(secondWorkerContext);
        close(firstWorkerContext);
        close(apiContext);
        PostgresTestcontainersConfig.dropSchema(POSTGRES, schema);
        Files.deleteIfExists(keyringPath);
    }

    @BeforeEach
    void clearState() {
        apiContext.getBean(NamedParameterJdbcTemplate.class).update(
            "TRUNCATE TABLE audit_event, run_trigger_idempotency, job_run, job_definition, "
                + "datasource_permission, managed_datasource CASCADE", Map.of());
    }

    @Test
    void dispatchesThroughApiAndExactlyOneOfTwoWorkersExecutesDespiteDuplicateWakeups() throws Exception {
        Path source = createDatabase("distributed-source.db", 2, false);
        Path sink = createDatabase("distributed-sink.db", 0, false);
        JobDefinition definition = definition(source, sink, ReplicationMode.COMPLETE, null, null,
                RetryPolicy.defaultsFor(ReplicationMode.COMPLETE));
        JobDefinition persisted = apiContext.getBean(JobDefinitionRepository.class).insert(definition);
        RunDispatchService dispatchService = apiContext.getBean(RunDispatchService.class);

        RunDispatchResult dispatch = dispatchService.dispatchManual(persisted.id(), "distributed-duplicate-key");
        JobRun pending = dispatch.run().orElseThrow();
        assertEquals(JobRunStatus.PENDING, pending.status());
        assertNull(pending.leaseToken());
        apiContext.getBean(PostgresNotificationPublisher.class).publishRun(pending.id());

        JobRun completed = awaitStatus(pending.id(), JobRunStatus.SUCCEEDED);

        assertEquals(JobRunStatus.SUCCEEDED, completed.status());
        assertTrue(Set.of(FIRST_WORKER, SECOND_WORKER).contains(completed.executorIdentity()));
        assertEquals(2, completed.rowsProcessed());
        assertEquals(1, countRuns(persisted.id()));
        assertEquals(2, countRows(sink, "orders_copy"));
    }

    @Test
    void commitsIncrementalWatermarkThroughWorkerAndLeavesApiReadingPostgresState() throws Exception {
        Path source = createDatabase("distributed-incremental-source.db", 2, true);
        Path sink = createDatabase("distributed-incremental-sink.db", 0, true);
        JobDefinition definition = definition(source, sink, ReplicationMode.INCREMENTAL,
                "updated_at", "0", RetryPolicy.defaultsFor(ReplicationMode.INCREMENTAL));
        JobDefinition persisted = apiContext.getBean(JobDefinitionRepository.class).insert(definition);

        JobRun run = apiContext.getBean(RunDispatchService.class)
                .dispatchScheduled(persisted.id()).run().orElseThrow();
        JobRun completed = awaitStatus(run.id(), JobRunStatus.SUCCEEDED);

        assertEquals("20", completed.committedWatermark());
        assertEquals("20", apiContext.getBean(JobRunRepository.class)
                .findLastCommittedWatermark(persisted.id()).orElseThrow());
        assertEquals(2, countRows(sink, "orders_copy"));
        assertNotNull(completed.finishedAt());
        assertTrue(completed.durationMillis() >= 0);
    }

    @Test
    void recoversExpiredAttemptAsANewWorkerAttemptAndFencesTheAbandonedToken() throws Exception {
        Path source = createDatabase("distributed-recovery-source.db", 2, false);
        Path sink = createDatabase("distributed-recovery-sink.db", 0, false);
        JobDefinition definition = definition(source, sink, ReplicationMode.COMPLETE, null, null,
                new RetryPolicy(3, 0, true));
        JobDefinition persisted = apiContext.getBean(JobDefinitionRepository.class).insert(definition);
        JobRun abandoned = apiContext.getBean(JobRunRepository.class).claimNextEligible(
                apiContext.getBean(JobRunRepository.class).insertPendingNow(persisted.id(), null, 1).id(),
                "lost-worker", Duration.ofSeconds(5)).orElseThrow();
        apiContext.getBean(NamedParameterJdbcTemplate.class).update(
                "UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
                Map.of("id", abandoned.id()));

        RunDispatchResult recovery = apiContext.getBean(RunDispatchService.class).recoverExpiredRun(abandoned.id());
        JobRun replacement = recovery.run().orElseThrow();
        JobRun completed = awaitStatus(replacement.id(), JobRunStatus.SUCCEEDED);

        assertTrue(recovery.replacementCreated());
        assertEquals(abandoned.id(), completed.previousRunId());
        assertEquals(2, completed.attempt());
        assertEquals(JobRunStatus.RETRY_SCHEDULED,
                apiContext.getBean(JobRunRepository.class).findById(abandoned.id()).orElseThrow().status());
            assertEquals(JobRunStore.FencedUpdateResult.FENCED,
                apiContext.getBean(RunFinalizationService.class).markSucceeded(
                    abandoned.id(), abandoned.leaseToken(), 99, 99, "stale"));
            assertEquals(2, countRuns(persisted.id()));
    }

    @Test
    void workerContextsHaveNoHttpSecurityOrQuartzSurface() {
        assertEquals(-1, firstWorkerContext.getEnvironment().getProperty("server.port", Integer.class));
        assertEquals(-1, secondWorkerContext.getEnvironment().getProperty("server.port", Integer.class));
        assertTrue(firstWorkerContext.getBeansOfType(SecurityFilterChain.class).isEmpty());
        assertTrue(firstWorkerContext.getBeansOfType(SessionRepository.class).isEmpty());
        assertTrue(firstWorkerContext.getBeansOfType(RunExecutionCoordinator.class).isEmpty());
        assertTrue(secondWorkerContext.getBeansOfType(SecurityFilterChain.class).isEmpty());
        assertTrue(secondWorkerContext.getBeansOfType(SessionRepository.class).isEmpty());
        assertTrue(secondWorkerContext.getBeansOfType(RunExecutionCoordinator.class).isEmpty());
        assertTrue(firstWorkerContext.containsBean("workerRuntimeLifecycle"));
        assertTrue(secondWorkerContext.containsBean("workerRuntimeLifecycle"));
    }

    private static ConfigurableApplicationContext application(String profile, String workerIdentity) {
        String[] properties = {
            "--spring.datasource.url=" + metadataUrl,
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--spring.flyway.enabled=false",
            "--replicadb.security.master-key-file=" + keyringPath,
            "--server.port=" + ("worker".equals(profile) ? "-1" : "0"),
            "--management.server.port=0",
            "--replicadb.server.local-execution.enabled=false",
            "--replicadb.worker.identity=" + (workerIdentity == null ? "" : workerIdentity),
            "--replicadb.worker.max-concurrent-runs=1",
            "--replicadb.worker.lease-duration=5s",
            "--replicadb.worker.heartbeat-interval=100ms",
            "--replicadb.worker.poll-interval=100ms",
            "--replicadb.worker.listener.initial-reconnect-delay=10ms",
            "--replicadb.worker.listener.max-reconnect-delay=100ms",
            "--replicadb.worker.shutdown-timeout=2s",
            "--spring.datasource.hikari.maximum-pool-size=8"
        };
        return new SpringApplicationBuilder(ReplicaDbServerApplication.class)
                .profiles(profile)
            .run(properties);
    }

    private JobRun awaitStatus(UUID runId, JobRunStatus expectedStatus) throws Exception {
        JobRunRepository repository = apiContext.getBean(JobRunRepository.class);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            JobRun run = repository.findById(runId).orElseThrow();
            if (run.status() == expectedStatus) {
                return run;
            }
            if (run.status().isTerminal() && run.status() != expectedStatus) {
                throw new AssertionError("Run reached unexpected status " + run.status());
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Run did not reach " + expectedStatus + ": " + runId);
    }

    private long countRuns(UUID jobDefinitionId) {
        return apiContext.getBean(NamedParameterJdbcTemplate.class).queryForObject(
                "SELECT COUNT(*) FROM job_run WHERE job_definition_id = :jobDefinitionId",
                Map.of("jobDefinitionId", jobDefinitionId), Long.class);
    }

        private JobDefinition definition(Path source, Path sink, ReplicationMode mode,
                         String watermarkColumn, String initialWatermark,
                         RetryPolicy retryPolicy) {
        ManagedDataSourceRepository dataSources = apiContext.getBean(ManagedDataSourceRepository.class);
        SecretProtectionService protectionService = apiContext.getBean(SecretProtectionService.class);
        UUID sourceId = ManagedDataSourceTestSupport.insert(dataSources, protectionService,
            "distributed-source-" + UUID.randomUUID(), ConnectorType.SQLITE, "jdbc:sqlite:" + source);
        UUID sinkId = ManagedDataSourceTestSupport.insert(dataSources, protectionService,
            "distributed-sink-" + UUID.randomUUID(), ConnectorType.SQLITE, "jdbc:sqlite:" + sink);
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("distributed-definition-" + UUID.randomUUID())
            .withSourceDatasourceId(sourceId)
                .withSourceTable("orders")
            .withSinkDatasourceId(sinkId)
                .withSinkTable("orders_copy")
                .withMode(mode)
                .withIncrementalWatermarkColumn(watermarkColumn)
                .withInitialWatermarkValue(initialWatermark)
                .withRetryPolicy(retryPolicy)
                .build();
    }

    private Path createDatabase(String filename, int rowCount, boolean incremental) throws SQLException {
        Path database = tempDirectory.resolve(filename);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            if (incremental) {
                statement.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, payload TEXT, updated_at INTEGER NOT NULL)");
                statement.execute("CREATE TABLE orders_copy (id INTEGER PRIMARY KEY, payload TEXT, updated_at INTEGER)");
                for (int index = 1; index <= rowCount; index++) {
                    statement.execute("INSERT INTO orders (id, payload, updated_at) VALUES (" + index
                            + ", 'payload-" + index + "', " + index * 10 + ")");
                }
            } else {
                statement.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, payload TEXT)");
                statement.execute("CREATE TABLE orders_copy (id INTEGER PRIMARY KEY, payload TEXT)");
                for (int index = 1; index <= rowCount; index++) {
                    statement.execute("INSERT INTO orders (id, payload) VALUES (" + index
                            + ", 'payload-" + index + "')");
                }
            }
        }
        return database;
    }

    private static long countRows(Path database, String tableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement();
             java.sql.ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static void close(ConfigurableApplicationContext context) {
        if (context != null) {
            context.close();
        }
    }
}
