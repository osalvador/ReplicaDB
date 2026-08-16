package org.replicadb.server.job.execution;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class RunExecutionCoordinatorTest {

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private JobExecutionService jobExecutionService;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @TempDir
    Path tempDirectory;

    private RunExecutionCoordinator coordinator;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
        coordinator = new RunExecutionCoordinator(jobRunRepository, jobExecutionService, 1);
    }

    @AfterEach
    void shutdownCoordinator() {
        coordinator.shutdown();
    }

    @Test
    void executesAClaimedRunAsynchronously() throws Exception {
        Path sourceDatabase = createDatabase("source-success.db", 2);
        Path sinkDatabase = createDatabase("sink-success.db", 0);
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase, ReplicationMode.COMPLETE);
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPending(persistedDefinition.id(), null, 1);

        coordinator.submit(pending.id(), "coordinator-worker");
        JobRun completed = awaitTerminal(pending.id());

        assertEquals(JobRunStatus.SUCCEEDED, completed.status());
        assertEquals(2, completed.rowsProcessed());
    }

    @Test
    void cancellationSignalsAnInFlightRun() throws Exception {
        Path sourceDatabase = createDatabase("source-cancel.db", 5000);
        Path sinkDatabase = createDatabase("sink-cancel.db", 0);
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase, ReplicationMode.COMPLETE);
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPending(persistedDefinition.id(), null, 1);

        coordinator.submit(pending.id(), "coordinator-worker");
        boolean cancellationDelivered = awaitCancellationRequest(pending.id());
        JobRun completed = awaitTerminal(pending.id());

        assertTrue(cancellationDelivered);
        assertEquals(JobRunStatus.CANCELLED, completed.status());
    }

    @Test
    void returnsFalseWhenNoRunIsInFlight() {
        assertFalse(coordinator.requestCancellation(UUID.randomUUID()));
    }

    @Test
    void shutdownAllowsAnAcceptedTaskToFinish() throws Exception {
        JobRunRepository repository = Mockito.mock(JobRunRepository.class);
        JobExecutionService executionService = Mockito.mock(JobExecutionService.class);
        CountDownLatch taskStarted = new CountDownLatch(1);
        when(repository.claimById(any(), anyString(), any(Duration.class))).thenAnswer(invocation -> {
            taskStarted.countDown();
            return Optional.empty();
        });
        RunExecutionCoordinator isolatedCoordinator = new RunExecutionCoordinator(repository, executionService, 1);

        isolatedCoordinator.submit(UUID.randomUUID(), "shutdown-worker");
        isolatedCoordinator.shutdown();

        assertTrue(taskStarted.await(2, TimeUnit.SECONDS));
        assertTrue(isolatedCoordinator.isShutdown());
    }

    private JobRun awaitTerminal(UUID runId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            JobRun run = jobRunRepository.findById(runId).orElseThrow();
            if (run.status().isTerminal()) {
                return run;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Run did not reach a terminal state: " + runId);
    }

    private boolean awaitCancellationRequest(UUID runId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            JobRun run = jobRunRepository.findById(runId).orElseThrow();
            if (run.status().isTerminal()) {
                return false;
            }
            if (coordinator.requestCancellation(runId)) {
                return true;
            }
            Thread.sleep(5);
        }
        return false;
    }

    private Path createDatabase(String filename, int rowCount) throws SQLException {
        Path database = tempDirectory.resolve(filename);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, payload TEXT)");
            statement.execute("CREATE TABLE orders_copy (id INTEGER PRIMARY KEY, payload TEXT)");
            for (int index = 1; index <= rowCount; index++) {
                statement.execute("INSERT INTO orders (id, payload) VALUES (" + index + ", 'payload-" + index + "')");
            }
        }
        return database;
    }

    private static JobDefinition jobDefinition(Path sourceDatabase, Path sinkDatabase, ReplicationMode mode) {
        return new JobDefinition(
                null, "job-" + UUID.randomUUID(), "jdbc:sqlite:" + sourceDatabase, null, null,
                "orders", null, "jdbc:sqlite:" + sinkDatabase, null, null, "orders_copy", mode, 1,
                null, null, null, null);
    }
}
