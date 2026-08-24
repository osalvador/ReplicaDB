package org.replicadb.server.job.execution;

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
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class WorkerExecutionIT {

    private static final String WORKER_IDENTITY = "worker-execution-it";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    private static String schema;
    private static String metadataUrl;
    private static ConfigurableApplicationContext workerContext;

    @TempDir
    Path tempDirectory;

    @BeforeAll
    static void startWorker() throws Exception {
        schema = PostgresTestcontainersConfig.isolatedSchema();
        PostgresTestcontainersConfig.migrate(POSTGRES, schema);
        metadataUrl = PostgresTestcontainersConfig.jdbcUrl(POSTGRES, schema);
        workerContext = new SpringApplicationBuilder(ReplicaDbServerApplication.class)
                .profiles("worker")
                .run(
                        "--spring.datasource.url=" + metadataUrl,
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.flyway.enabled=false",
                        "--replicadb.worker.identity=" + WORKER_IDENTITY,
                        "--replicadb.worker.max-concurrent-runs=1",
                        "--replicadb.worker.lease-duration=5s",
                        "--replicadb.worker.heartbeat-interval=100ms",
                        "--replicadb.worker.poll-interval=100ms",
                        "--replicadb.worker.listener.initial-reconnect-delay=10ms",
                        "--replicadb.worker.listener.max-reconnect-delay=100ms",
                        "--replicadb.worker.shutdown-timeout=2s",
                        "--spring.datasource.hikari.maximum-pool-size=8");
    }

    @AfterAll
    static void stopWorker() throws Exception {
        if (workerContext != null) {
            workerContext.close();
        }
        PostgresTestcontainersConfig.dropSchema(POSTGRES, schema);
    }

    @BeforeEach
    void clearState() {
        workerContext.getBean(NamedParameterJdbcTemplate.class).update(
                "TRUNCATE TABLE audit_event, run_trigger_idempotency, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void preservesTheLastCommittedWatermarkWhenTheNextWorkerAttemptFails() throws Exception {
        Path source = createDatabase("worker-watermark-source.db", 2, true);
        Path sink = createDatabase("worker-watermark-sink.db", 0, true);
        JobDefinition definition = definition(source, sink, ReplicationMode.INCREMENTAL,
                "updated_at", "0", RetryPolicy.defaultsFor(ReplicationMode.INCREMENTAL));
        JobDefinitionRepository definitions = workerContext.getBean(JobDefinitionRepository.class);
        JobDefinition persisted = definitions.insert(definition);
        RunDispatchService dispatchService = workerContext.getBean(RunDispatchService.class);

        JobRun first = dispatchService.dispatchManual(persisted.id(), "watermark-success").run().orElseThrow();
        assertEquals(JobRunStatus.SUCCEEDED, awaitStatus(first.id(), JobRunStatus.SUCCEEDED).status());
        assertEquals("20", workerContext.getBean(JobRunRepository.class)
                .findLastCommittedWatermark(persisted.id()).orElseThrow());

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + source);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE orders");
        }
        JobRun second = dispatchService.dispatchManual(persisted.id(), "watermark-failure").run().orElseThrow();

        assertEquals(JobRunStatus.FAILED, awaitStatus(second.id(), JobRunStatus.FAILED).status());
        assertNull(workerContext.getBean(JobRunRepository.class).findById(second.id()).orElseThrow()
                .committedWatermark());
        assertEquals("20", workerContext.getBean(JobRunRepository.class)
                .findLastCommittedWatermark(persisted.id()).orElseThrow());
    }

    @Test
    void retriesAFailedRunAsANewAttemptFromTheBeginning() throws Exception {
        Path source = createDatabase("worker-retry-source.db", 2, false);
        Path sink = createDatabase("worker-retry-sink.db", 0, false);
        JobDefinition definition = definition(source, sink, ReplicationMode.COMPLETE, null, null,
                new RetryPolicy(3, 0, true));
        JobDefinition persisted = workerContext.getBean(JobDefinitionRepository.class).insert(definition);
        JobRunRepository runs = workerContext.getBean(JobRunRepository.class);
        RunLeaseService leases = workerContext.getBean(RunLeaseService.class);
        JobRun failed = leases.claimRequested(
                runs.insertPendingNow(persisted.id(), null, 1).id(), "failed-before-retry", Duration.ofSeconds(5))
                .orElseThrow();
        runs.markFailed(failed.id(), failed.leaseToken(), 0, 0, "controlled failure");

        RunDispatchResult retry = workerContext.getBean(RunDispatchService.class).dispatchRetry(failed.id());
        JobRun replacement = awaitStatus(retry.run().orElseThrow().id(), JobRunStatus.SUCCEEDED);

        assertEquals(JobRunStatus.SUCCEEDED, replacement.status());
        assertEquals(failed.id(), replacement.previousRunId());
        assertEquals(2, replacement.attempt());
        assertEquals(JobRunStatus.RETRY_SCHEDULED, runs.findById(failed.id()).orElseThrow().status());
        assertEquals(2, countRuns(persisted.id()));
        assertEquals(2, countRows(sink, "orders_copy"));
    }

    private JobRun awaitStatus(UUID runId, JobRunStatus expectedStatus) throws Exception {
        JobRunRepository runs = workerContext.getBean(JobRunRepository.class);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            JobRun run = runs.findById(runId).orElseThrow();
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
        return workerContext.getBean(NamedParameterJdbcTemplate.class).queryForObject(
                "SELECT COUNT(*) FROM job_run WHERE job_definition_id = :jobDefinitionId",
                Map.of("jobDefinitionId", jobDefinitionId), Long.class);
    }

    private static JobDefinition definition(Path source, Path sink, ReplicationMode mode,
                                            String watermarkColumn, String initialWatermark,
                                            RetryPolicy retryPolicy) {
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("worker-execution-" + UUID.randomUUID())
                .withSourceConnect("jdbc:sqlite:" + source)
                .withSourceTable("orders")
                .withSinkConnect("jdbc:sqlite:" + sink)
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
}