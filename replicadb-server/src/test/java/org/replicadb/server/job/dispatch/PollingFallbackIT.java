package org.replicadb.server.job.dispatch;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.config.WorkerRuntimeProperties;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.execution.ActiveRunRegistry;
import org.replicadb.server.job.execution.HeartbeatHandle;
import org.replicadb.server.job.execution.HeartbeatService;
import org.replicadb.server.job.execution.JobExecutionService;
import org.replicadb.server.job.execution.JobRunOutcome;
import org.replicadb.server.job.execution.RunExecutionHandle;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.execution.WorkerRunIdentity;
import org.replicadb.server.job.execution.WorkerAdmissionPolicy;
import org.replicadb.server.job.execution.WorkerAdmissionScheduler;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.observability.WorkerBusySlotTracker;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class PollingFallbackIT {

    private static final String WORKER_IDENTITY = "polling-it-worker";

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private RunLeaseService runLeaseService;

    @Autowired
    private RunDispatchService runDispatchService;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    private final List<PollingFallback> pollingInstances = new ArrayList<>();
    private final List<WorkerDispatchCoordinator> coordinators = new ArrayList<>();

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition, datasource_permission, "
            + "managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @AfterEach
    void stopRuntime() {
        pollingInstances.forEach(PollingFallback::stop);
        coordinators.forEach(coordinator -> coordinator.shutdown(Duration.ofSeconds(2)));
        pollingInstances.clear();
        coordinators.clear();
    }

    @Test
    void startupScanClaimsWorkCreatedBeforeTheWorkerStarts() throws Exception {
        JobRun pending = pendingRun();
        ActiveRunRegistry registry = new ActiveRunRegistry();
        WorkerDispatchCoordinator coordinator = coordinator(registry);
        PollingFallback polling = polling(coordinator, Duration.ofHours(1));

        polling.start();

        assertEquals(JobRunStatus.RUNNING, awaitRun(pending.id()).status());
        assertEquals(WORKER_IDENTITY, awaitRun(pending.id()).executorIdentity());
    }

    @Test
    void periodicScanClaimsWorkWhenTheStartupScanFoundNothing() throws Exception {
        ActiveRunRegistry registry = new ActiveRunRegistry();
        WorkerDispatchCoordinator coordinator = coordinator(registry);
        PollingFallback polling = polling(coordinator, Duration.ofMillis(25));
        polling.start();
        JobRun pending = pendingRun();

        assertEquals(JobRunStatus.RUNNING, awaitRun(pending.id()).status());
    }

    @Test
    void pollingDeliversCancellationAfterTheControlNotificationWasMissed() throws Exception {
        ActiveRunRegistry registry = new ActiveRunRegistry();
        WorkerDispatchCoordinator coordinator = coordinator(registry);
        PollingFallback polling = polling(coordinator, Duration.ofMillis(25));
        JobRun pending = pendingRun();
        JobRun claimed = runLeaseService.claimRequested(
                pending.id(), WORKER_IDENTITY, Duration.ofMinutes(5)).orElseThrow();
        RunExecutionHandle handle = new RunExecutionHandle(claimed, options());
        registry.register(handle);
        polling.start();

        jobRunRepository.requestCancellation(claimed.id(), "polling cancellation");

        await(() -> handle.cancellationContext().isCancellationRequested());
        assertTrue(handle.cancellationContext().isCancellationRequested());
    }

    @Test
    void concurrentExpiryScansCreateOneReplacementAndOneNotification() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("polling-recovery-" + UUID.randomUUID())
            .withDefaultDatasourceReferences()
                .withRetryPolicy(new RetryPolicy(3, 0, true))
                .build());
        JobRun claimed = runLeaseService.claimRequested(
                jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
                "expired-worker", Duration.ofMinutes(5)).orElseThrow();
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
                Map.of("id", claimed.id()));
        WorkerDispatchCoordinator firstCoordinator = coordinator(new ActiveRunRegistry());
        WorkerDispatchCoordinator secondCoordinator = coordinator(new ActiveRunRegistry());
        PollingFallback firstPolling = polling(firstCoordinator, Duration.ofHours(1));
        PollingFallback secondPolling = polling(secondCoordinator, Duration.ofHours(1));

        try (Connection listener = listeningConnection(RunNotificationPublisher.RUN_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = executor.submit(firstPolling::start);
                Future<?> second = executor.submit(secondPolling::start);
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }

            assertEquals(1, jdbcTemplate.queryForObject("""
                    SELECT COUNT(*) FROM job_run
                    WHERE job_definition_id = :jobDefinitionId AND status = 'PENDING'
                    """, Map.of("jobDefinitionId", definition.id()), Integer.class));
            assertEquals(1, notifications(pgConnection, 2_000).stream()
                    .filter(notification -> notification.getParameter() != null)
                    .count());
        }
    }

    private PollingFallback polling(WorkerDispatchCoordinator coordinator, Duration interval) {
        PollingFallback polling = new PollingFallback(coordinator, jobRunRepository, runDispatchService,
                WORKER_IDENTITY, interval, 10, PollingFallback.newScheduler(), Duration.ofSeconds(2));
        pollingInstances.add(polling);
        return polling;
    }

    private WorkerDispatchCoordinator coordinator(ActiveRunRegistry registry) {
        JobExecutionService executionService = mock(JobExecutionService.class);
        HeartbeatService heartbeatService = mock(HeartbeatService.class);
        when(heartbeatService.start(any())).thenReturn(mock(HeartbeatHandle.class));
        doAnswer(invocation -> {
            ClaimedRunPreparation preparation = invocation.getArgument(0);
            JobRun run = preparation.run();
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            registry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(executionService).executeClaimedRun(any(), any());
        WorkerDispatchCoordinator coordinator = new WorkerDispatchCoordinator(
                runLeaseService, jobRunRepository, executionService, registry, heartbeatService,
                new WorkerRunIdentity(WORKER_IDENTITY), 1,
                Duration.ofMinutes(5), Duration.ofSeconds(2), metrics(), policy(),
                new WorkerAdmissionScheduler(), tracker(), 1_024);
        coordinators.add(coordinator);
        return coordinator;
    }

    private static ManagedRuntimeMetrics metrics() {
        return new ManagedRuntimeMetrics(new SimpleMeterRegistry());
    }

    private static WorkerBusySlotTracker tracker() {
        return new WorkerBusySlotTracker(new SimpleMeterRegistry(), WORKER_IDENTITY, 1, System::nanoTime);
    }

    private static WorkerAdmissionPolicy policy() {
        WorkerRuntimeProperties.Admission admission = new WorkerRuntimeProperties.Admission();
        admission.setJitterMax(Duration.ZERO);
        admission.setGenericCooldown(Duration.ZERO);
        admission.getAdaptiveBackoff().setEnabled(false);
        return new WorkerAdmissionPolicy(admission, System::nanoTime, () -> 0.0);
    }

    private JobRun pendingRun() {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("polling-job-" + UUID.randomUUID())
            .withDefaultDatasourceReferences()
                .build());
        return jobRunRepository.insertPendingNow(definition.id(), null, 1);
    }

    private JobRun awaitRun(UUID runId) throws Exception {
        final JobRun[] result = new JobRun[1];
        await(() -> {
            result[0] = jobRunRepository.findById(runId).orElseThrow();
            return result[0].status() == JobRunStatus.RUNNING;
        });
        return result[0];
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

    private static ToolOptions options() {
        try {
            return new ToolOptions(new String[]{
                    "--source-connect", "jdbc:sqlite:source.db",
                    "--sink-connect", "jdbc:sqlite:sink.db"
            });
        } catch (Exception exception) {
            throw new IllegalStateException("Could not create test options", exception);
        }
    }

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && !check.completed()) {
            Thread.sleep(10);
        }
        assertTrue(check.completed());
    }

    @FunctionalInterface
    private interface Check {
        boolean completed();
    }
}
