package org.replicadb.server.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectionCredentials;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.application.RunFinalizationService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doAnswer;
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
    private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @SpyBean
    private JobDefinitionOptionsFileWriter optionsFileWriter;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void executesIncrementalRunAndPersistsReducedWatermark() throws Exception {
        Path sourceDatabase = createDatabase("source-success.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-success.db", true, "orders_copy");
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "orders_copy", ReplicationMode.INCREMENTAL, "updated_at", "0");
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.SUCCEEDED, outcome.status());
        assertEquals(2, outcome.rowsProcessed());
        assertEquals(JobRunStatus.SUCCEEDED, persistedRun.status());
        assertEquals("20", persistedRun.committedWatermark());
        assertEquals("20", jobRunRepository.findLastCommittedWatermark(persistedDefinition.id()).orElseThrow());
        assertEquals(2, countRows(sinkDatabase, "orders_copy"));
        assertNotNull(persistedRun.finishedAt());
        AuditEvent event = terminalEvent(AuditAction.RUN_SUCCEEDED, pending.id());
        assertEquals(Long.toString(persistedRun.rowsProcessed()), event.detail().get("rowsProcessed"));
        assertTrue(event.actor().username().startsWith("system:"));
        assertFalse(event.actor().username().equals("integration-worker"));
    }

    @Test
    void failedRunPreservesPreviouslyCommittedWatermark() throws Exception {
        Path sourceDatabase = createDatabase("source-failure.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-failure.db", false, null);
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "missing_sink", ReplicationMode.INCREMENTAL, "updated_at", "0");
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);
        JobRun previousRunning = jobRunRepository.claimNextEligible(null, "previous-worker", java.time.Duration.ofMinutes(5))
                .orElseThrow();
        jobRunRepository.markSucceeded(previousRunning.id(), previousRunning.leaseToken(), 2, 10, "15");
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), previousRunning.id(), 2);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.FAILED, outcome.status());
        assertEquals(JobRunStatus.FAILED, persistedRun.status());
        assertFalse(persistedRun.errorMessage().isBlank());
        assertEquals("15", jobRunRepository.findLastCommittedWatermark(persistedDefinition.id()).orElseThrow());
        AuditEvent event = terminalEvent(AuditAction.RUN_FAILED, pending.id());
        assertEquals(persistedRun.errorMessage(), event.detail().get("errorMessage"));
        assertFalse(event.detail().toString().contains("${env:"));
        assertTrue(event.actor().username().startsWith("system:"));
    }

    @Test
    void missingEnvironmentReferenceFailsWithoutPersistingResolvedSecrets() {
        JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
            .withName("missing-env-" + UUID.randomUUID())
            .withSourceConnect("${env:UNSET_SOURCE_CONNECT}")
            .withSourceTable("orders")
            .withSinkTable("orders_copy")
            .build();
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.FAILED, outcome.status());
        assertEquals(JobRunStatus.FAILED, persistedRun.status());
        assertEquals("Missing environment variable: UNSET_SOURCE_CONNECT", persistedRun.errorMessage());
        assertFalse(persistedRun.errorMessage().contains("jdbc:sink"));
    }

    @Test
    void emptyQueueDoesNotMarkAnyRun() {
        JobRunStore runStore = Mockito.mock(JobRunStore.class);
        JobDefinitionStore definitions = Mockito.mock(JobDefinitionStore.class);
        when(runStore.claimNextEligible(isNull(), eq("integration-worker"), any())).thenReturn(Optional.empty());
        JobExecutionService service = new JobExecutionService(runStore, definitions,
            new RunLeaseService(runStore), new RunFinalizationService(runStore),
            new JobDefinitionEnvResolver(), new JobDefinitionOptionsFileWriter(),
            Mockito.mock(AuditService.class), Mockito.mock(AuditActorResolver.class), new ActiveRunRegistry());

        assertTrue(service.executeNextPending("integration-worker").isEmpty());
        verify(runStore, never()).markSucceeded(any(), any(), any(Long.class), any(Long.class), any());
        verify(runStore, never()).markFailed(any(), any(), any(Long.class), any(Long.class), anyString());
        verify(runStore, never()).markCancelled(any(), any(), any(Long.class), any(Long.class));
    }

        @Test
        void invokesStartedCallbackWithOptionsBeforeReturningOutcome() throws Exception {
        Path sourceDatabase = createDatabase("source-callback.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-callback.db", true, "orders_copy");
        JobDefinition definition = jobDefinitionWithConnectionParams(sourceDatabase, sinkDatabase);
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(null, "callback-worker", java.time.Duration.ofMinutes(5))
            .orElseThrow();
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<RunExecutionHandle> startedHandle = new AtomicReference<>();

        JobRunOutcome outcome = executionService.executeClaimedRun(claimed, handle -> {
            callbackCount.incrementAndGet();
            startedHandle.set(handle);
            assertTrue(executionService.activeRunRegistry().find(claimed.id()).isPresent());
        });

        assertEquals(claimed.id(), outcome.runId());
        assertEquals(1, callbackCount.get());
        assertTrue(startedHandle.get() != null);
        assertEquals(claimed.id(), startedHandle.get().runId());
        assertEquals(claimed.leaseToken(), startedHandle.get().leaseToken());
        assertEquals("ReplicaDB", startedHandle.get().toolOptions()
            .getSourceConnectionParams().getProperty("ApplicationName"));
        assertEquals("100", startedHandle.get().toolOptions()
            .getSinkConnectionParams().getProperty("batch.size"));
        assertTrue(executionService.activeRunRegistry().find(claimed.id()).isEmpty());
        assertEquals(JobRunStatus.SUCCEEDED, jobRunRepository.findById(claimed.id()).orElseThrow().status());
        }

    @Test
    void deletesOptionsFileAfterSuccessfulAndFailedRuns() throws Exception {
        AtomicReference<Path> writtenPath = new AtomicReference<>();
        doAnswer(invocation -> {
            Path path = (Path) invocation.callRealMethod();
            writtenPath.set(path);
            return path;
        }).when(optionsFileWriter).write(any(JobDefinition.class), any(), any());

        Path sourceDatabase = createDatabase("source-cleanup.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-cleanup.db", true, "orders_copy");
        JobDefinition successfulDefinition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "orders_copy", ReplicationMode.COMPLETE, null, null);
        JobDefinition persistedSuccessful = jobDefinitionRepository.insert(successfulDefinition);
        JobRun successfulRun = jobRunRepository.insertPendingNow(persistedSuccessful.id(), null, 1);
        executionService.executeNextPending("cleanup-worker").orElseThrow();
        Path successfulPath = writtenPath.get();

        assertNotNull(successfulPath);
        assertFalse(Files.exists(successfulPath));

        Path failingSink = tempDirectory.resolve("sink-cleanup-failure.db");
        JobDefinition failingDefinition = jobDefinition(sourceDatabase, failingSink,
                "orders", "missing_sink", ReplicationMode.COMPLETE, null, null);
        JobDefinition persistedFailing = jobDefinitionRepository.insert(failingDefinition);
        jobRunRepository.insertPendingNow(persistedFailing.id(), null, 1);
        executionService.executeNextPending("cleanup-worker").orElseThrow();
        Path failingPath = writtenPath.get();

        assertNotNull(failingPath);
        assertFalse(Files.exists(failingPath));
        assertEquals(JobRunStatus.SUCCEEDED, jobRunRepository.findById(successfulRun.id()).orElseThrow().status());
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

    private AuditEvent terminalEvent(AuditAction action, UUID runId) {
        return auditEventRepository.findPage(new AuditEventFilter(null, action,
                AuditResourceType.JOB_RUN, runId.toString(), null, null), 0, 50).get(0);
    }

    private static JobDefinition jobDefinition(Path sourceDatabase, Path sinkDatabase,
                                                String sourceTable, String sinkTable,
                                                ReplicationMode mode, String watermarkColumn,
                                                String initialWatermarkValue) {
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName("job-" + UUID.randomUUID())
            .withSourceConnect("jdbc:sqlite:" + sourceDatabase)
            .withSourceTable(sourceTable)
            .withSinkConnect("jdbc:sqlite:" + sinkDatabase)
            .withSinkTable(sinkTable)
            .withMode(mode)
            .withIncrementalWatermarkColumn(watermarkColumn)
            .withInitialWatermarkValue(initialWatermarkValue)
            .build();
    }

            private static JobDefinition jobDefinitionWithConnectionParams(Path sourceDatabase, Path sinkDatabase) {
            return new JobDefinition(
                null, "job-" + UUID.randomUUID(),
                new SourceEndpoint(new ConnectionCredentials("jdbc:sqlite:" + sourceDatabase, null, null, null,
                    Map.of("ApplicationName", "ReplicaDB")), "orders", null, null, null),
                new SinkEndpoint(new ConnectionCredentials("jdbc:sqlite:" + sinkDatabase, null, null, null,
                    Map.of("batch.size", "100")), "orders_copy", null, null, false, false),
                ReplicationMode.COMPLETE, 1, null, null, null, null, 100, 0, false);
            }
}
