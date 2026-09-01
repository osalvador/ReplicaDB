package org.replicadb.server.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.api.DatasourceMapper;
import org.replicadb.server.job.api.DatasourceRequest;
import org.replicadb.server.job.application.RunFinalizationService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.application.RunPreparationService;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
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
    private ManagedDataSourceRepository dataSourceRepository;

    @Autowired
    private DatasourceMapper datasourceMapper;

    @Autowired
    private SecretProtectionService protectionService;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, datasource_permission, job_run, job_definition, "
                + "managed_datasource CASCADE", Map.of());
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
        assertEquals(persistedDefinition.sourceDatasourceId(), persistedRun.resolvedSourceDatasourceId());
        assertEquals(persistedDefinition.sinkDatasourceId(), persistedRun.resolvedSinkDatasourceId());
        assertNotNull(persistedRun.datasourcesResolvedAt());
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
        JobRun firstPending = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);
        ClaimedRunPreparation previous = jobRunRepository.claimAndPrepare(firstPending.id(), "previous-worker",
                Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(previous.run().id(), previous.run().leaseToken(), 2, 10, "15");
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), firstPending.id(), 2);

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
    void tamperedDatasourceBundleFailsBeforeCoreWithoutPersistingSecrets() throws Exception {
        Path sourceDatabase = createDatabase("source-tampered.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-tampered.db", true, "orders_copy");
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "orders_copy", ReplicationMode.COMPLETE, null, null);
        ManagedDataSource source = dataSourceRepository.findById(definition.sourceDatasourceId()).orElseThrow();
        EncryptedSecurityBundle bundle = protectionService.deserialize(source.encryptedSecurity());
        byte[] tamperedCiphertext = bundle.ciphertext();
        tamperedCiphertext[tamperedCiphertext.length - 1] ^= 1;
        byte[] tamperedEnvelope = protectionService.serialize(new EncryptedSecurityBundle(
                bundle.formatVersion(), bundle.algorithm(), bundle.keyVersion(),
                bundle.wrappedDataKey(), bundle.nonce(), tamperedCiphertext));
        dataSourceRepository.update(new ManagedDataSource(source.id(), source.name(), source.connectorType(),
                source.safeConnectDisplay(), source.technicalParams(), tamperedEnvelope,
                source.securityFormatVersion(), source.encryptionAlgorithm(), source.keyVersion(),
                source.createdAt(), source.updatedAt()));
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);

        JobRunOutcome outcome = executionService.executeNextPending("integration-worker").orElseThrow();
        JobRun persistedRun = jobRunRepository.findById(pending.id()).orElseThrow();

        assertEquals(JobRunStatus.FAILED, outcome.status());
        assertTrue(persistedRun.errorMessage().contains("Could not decrypt datasource security bundle"));
        assertFalse(persistedRun.errorMessage().contains("jdbc:sqlite:"));
    }

    @Test
    void emptyQueueDoesNotMarkAnyRun() {
        JobRunStore runStore = mock(JobRunStore.class);
        when(runStore.claimAndPrepare(isNull(), anyString(), any())).thenReturn(Optional.empty());
        JobExecutionService service = new JobExecutionService(runStore,
                new RunPreparationService(new RunLeaseService(runStore)),
                new RunFinalizationService(runStore),
                new DatasourceResolutionService(protectionService), new ManagedToolOptionsFactory(),
                mock(AuditService.class), new AuditActorResolver(), new ActiveRunRegistry());

        assertTrue(service.executeNextPending("integration-worker").isEmpty());
        verify(runStore, never()).markSucceeded(any(), any(), anyLong(), anyLong(), any());
        verify(runStore, never()).markFailed(any(), any(), anyLong(), anyLong(), anyString());
        verify(runStore, never()).markCancelled(any(), any(), anyLong(), anyLong());
    }

    @Test
    void invokesStartedCallbackWithOptionsBeforeCoreExecution() throws Exception {
        Path sourceDatabase = createDatabase("source-callback.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-callback.db", true, "orders_copy");
        JobDefinition definition = jobDefinitionWithConnectionParams(sourceDatabase, sinkDatabase);
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun pending = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);
        ClaimedRunPreparation claimed = jobRunRepository.claimAndPrepare(pending.id(), "callback-worker",
                Duration.ofMinutes(5)).orElseThrow();
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<RunExecutionHandle> startedHandle = new AtomicReference<>();

        JobRunOutcome outcome = executionService.executeClaimedRun(claimed, handle -> {
            callbackCount.incrementAndGet();
            startedHandle.set(handle);
            assertTrue(executionService.activeRunRegistry().find(claimed.run().id()).isPresent());
        });

        assertEquals(claimed.run().id(), outcome.runId());
        assertEquals(1, callbackCount.get());
        assertNotNull(startedHandle.get());
        assertEquals(claimed.run().leaseToken(), startedHandle.get().leaseToken());
        assertEquals("ReplicaDB", startedHandle.get().toolOptions()
                .getSourceConnectionParams().getProperty("ApplicationName"));
        assertEquals("100", startedHandle.get().toolOptions()
                .getSinkConnectionParams().getProperty("batch.size"));
        assertTrue(executionService.activeRunRegistry().find(claimed.run().id()).isEmpty());
        assertEquals(JobRunStatus.SUCCEEDED, jobRunRepository.findById(claimed.run().id()).orElseThrow().status());
    }

    @Test
    void doesNotCreateManagedOptionsFiles() throws Exception {
        Path sourceDatabase = createDatabase("source-no-options.db", true, "orders");
        Path sinkDatabase = createDatabase("sink-no-options.db", true, "orders_copy");
        JobDefinition definition = jobDefinition(sourceDatabase, sinkDatabase,
                "orders", "orders_copy", ReplicationMode.COMPLETE, null, null);
        JobDefinition persistedDefinition = jobDefinitionRepository.insert(definition);
        JobRun run = jobRunRepository.insertPendingNow(persistedDefinition.id(), null, 1);

        executionService.executeNextPending("no-options-worker").orElseThrow();

        assertEquals(JobRunStatus.SUCCEEDED, jobRunRepository.findById(run.id()).orElseThrow().status());
        try (var paths = Files.list(tempDirectory)) {
            assertTrue(paths.noneMatch(path -> path.getFileName().toString().startsWith("replicadb-job-")));
        }
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

    private JobDefinition jobDefinition(Path sourceDatabase, Path sinkDatabase,
                                        String sourceTable, String sinkTable,
                                        ReplicationMode mode, String watermarkColumn,
                                        String initialWatermarkValue) {
        UUID sourceId = insertDatasource("source-" + UUID.randomUUID(), "jdbc:sqlite:" + sourceDatabase, Map.of());
        UUID sinkId = insertDatasource("sink-" + UUID.randomUUID(), "jdbc:sqlite:" + sinkDatabase, Map.of());
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("job-" + UUID.randomUUID())
                .withSourceDatasourceId(sourceId)
                .withSourceTable(sourceTable)
                .withSinkDatasourceId(sinkId)
                .withSinkTable(sinkTable)
                .withMode(mode)
                .withIncrementalWatermarkColumn(watermarkColumn)
                .withInitialWatermarkValue(initialWatermarkValue)
                .build();
    }

    private JobDefinition jobDefinitionWithConnectionParams(Path sourceDatabase, Path sinkDatabase) {
        UUID sourceId = insertDatasource("source-params-" + UUID.randomUUID(), "jdbc:sqlite:" + sourceDatabase,
                Map.of("ApplicationName", "ReplicaDB"));
        UUID sinkId = insertDatasource("sink-params-" + UUID.randomUUID(), "jdbc:sqlite:" + sinkDatabase,
                Map.of("batch.size", "100"));
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("job-params-" + UUID.randomUUID())
                .withSourceDatasourceId(sourceId)
                .withSourceTable("orders")
                .withSinkDatasourceId(sinkId)
                .withSinkTable("orders_copy")
                .build();
    }

    private UUID insertDatasource(String name, String connect, Map<String, String> technicalParams) {
        UUID id = UUID.randomUUID();
        DatasourceRequest request = new DatasourceRequest(name, ConnectorType.SQLITE.getWireValue(), technicalParams,
                Map.of("connect", connect), Set.of());
        var bundle = protectionService.encrypt(id, request.security());
        dataSourceRepository.insert(datasourceMapper.toDataSource(id, request, request.security(), bundle,
                protectionService.serialize(bundle), null, null));
        return id;
    }
}
