package org.replicadb.server.job.execution;

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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobExecutionServiceIT {

    @Autowired
    private JobExecutionService executionService;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void executesIncrementalRunAndPersistsReducedWatermark() throws Exception {
        Path sourceDatabase = createDatabase("source-success.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-success.db", true, "orders_copy");
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "orders_copy", ReplicationMode.INCREMENTAL, "updated_at", "0");
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPending(persistedDefinition.id(), null, 1);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.SUCCEEDED, outcome.status());
        assertEquals(2, outcome.rowsProcessed());
        assertEquals(JobRunStatus.SUCCEEDED, persistedRun.status());
        assertEquals("20", persistedRun.committedWatermark());
        assertEquals("20", jobRunRepository.findLastCommittedWatermark(persistedDefinition.id()).orElseThrow());
        assertEquals(2, countRows(sinkDatabase, "orders_copy"));
        assertNotNull(persistedRun.finishedAt());
    }

    @Test
    void failedRunPreservesPreviouslyCommittedWatermark() throws Exception {
        Path sourceDatabase = createDatabase("source-failure.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-failure.db", false, null);
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "missing_sink", ReplicationMode.INCREMENTAL, "updated_at", "0");
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        jobRunRepository.insertPending(persistedDefinition.id(), null, 1);
        JobRun previousRunning = jobRunRepository.claimNextPending("previous-worker", java.time.Duration.ofMinutes(5))
                .orElseThrow();
        jobRunRepository.markSucceeded(previousRunning.id(), 2, 10, "15");
        JobRun pending = jobRunRepository.insertPending(persistedDefinition.id(), previousRunning.id(), 2);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.FAILED, outcome.status());
        assertEquals(JobRunStatus.FAILED, persistedRun.status());
        assertFalse(persistedRun.errorMessage().isBlank());
        assertEquals("15", jobRunRepository.findLastCommittedWatermark(persistedDefinition.id()).orElseThrow());
    }

    @Test
    void missingEnvironmentReferenceFailsWithoutPersistingResolvedSecrets() {
        JobDefinition definition = new JobDefinition(
                null, "missing-env-" + UUID.randomUUID(), "${env:UNSET_SOURCE_CONNECT}", null, null,
                "orders", null, "jdbc:sink", null, null, "orders_copy", ReplicationMode.COMPLETE, 1,
                null, null, Instant.now(), Instant.now());
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPending(persistedDefinition.id(), null, 1);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.FAILED, outcome.status());
        assertEquals(JobRunStatus.FAILED, persistedRun.status());
        assertEquals("Missing environment variable: UNSET_SOURCE_CONNECT", persistedRun.errorMessage());
        assertFalse(persistedRun.errorMessage().contains("jdbc:sink"));
    }

    @Test
    void emptyQueueDoesNotMarkAnyRun() {
        JobRunRepository repository = Mockito.mock(JobRunRepository.class);
        JobDefinitionRepository definitions = Mockito.mock(JobDefinitionRepository.class);
        when(repository.claimNextPending(anyString(), any())).thenReturn(Optional.empty());
        JobExecutionService service = new JobExecutionService(repository, definitions,
                new JobDefinitionEnvResolver(), new ToolOptionsArgsBuilder());

        assertTrue(service.executeNextPending("integration-worker").isEmpty());
        verify(repository, never()).markSucceeded(any(), any(Long.class), any(Long.class), any());
        verify(repository, never()).markFailed(any(), any(Long.class), any(Long.class), anyString());
        verify(repository, never()).markCancelled(any(), any(Long.class), any(Long.class));
    }

    private Path createDatabase(String filename, boolean createTable, String tableName) throws SQLException {
        Path database = tempDirectory.resolve(filename);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            if (createTable) {
                statement.execute("CREATE TABLE " + tableName
                        + " (id INTEGER PRIMARY KEY, payload TEXT, updated_at INTEGER NOT NULL)");
                statement.execute("INSERT INTO " + tableName
                        + " (id, payload, updated_at) VALUES (1, 'one', 10), (2, 'two', 20)");
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

    private static JobDefinition jobDefinition(Path sourceDatabase, Path sinkDatabase,
                                                String sourceTable, String sinkTable,
                                                ReplicationMode mode, String watermarkColumn,
                                                String initialWatermarkValue) {
        return new JobDefinition(
                null, "job-" + UUID.randomUUID(), "jdbc:sqlite:" + sourceDatabase, null, null,
                sourceTable, null, "jdbc:sqlite:" + sinkDatabase, null, null, sinkTable, mode, 1,
                watermarkColumn, initialWatermarkValue, null, null);
    }
}