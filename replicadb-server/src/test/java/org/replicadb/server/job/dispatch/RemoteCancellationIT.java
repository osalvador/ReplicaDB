package org.replicadb.server.job.dispatch;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.application.RunCancellationService;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.application.RunFinalizationService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.config.WorkerRuntimeProperties;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
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
import org.replicadb.server.job.persistence.PostgresNotificationPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
class RemoteCancellationIT {

    private static final String WORKER_IDENTITY = "remote-cancellation-worker";

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private RunLeaseService runLeaseService;

    @Autowired
    private RunFinalizationService runFinalizationService;

    @Autowired
    private RunCancellationService runCancellationService;

    @Autowired
    private RunDispatchService runDispatchService;

    @Autowired
    private PostgresNotificationPublisher notificationPublisher;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private final CopyOnWriteArrayList<WorkerDispatchCoordinator> coordinators = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PollingFallback> pollers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PostgreSQLNotificationListener> listeners = new CopyOnWriteArrayList<>();

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition, datasource_permission, "
            + "managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @AfterEach
    void stopRuntime() {
        listeners.forEach(PostgreSQLNotificationListener::stop);
        pollers.forEach(PollingFallback::stop);
        coordinators.forEach(coordinator -> coordinator.shutdown(Duration.ofSeconds(2)));
        listeners.clear();
        pollers.clear();
        coordinators.clear();
    }

    @Test
    void controlNotificationSignalsTheOwningWorkerAndFinishesCancelled() throws Exception {
        ActiveRunRegistry registry = new ActiveRunRegistry();
        CountDownLatch executionStarted = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        WorkerDispatchCoordinator coordinator = coordinator(registry, executionStarted, cancellationObserved);
        PollingFallback polling = mock(PollingFallback.class);
        CountDownLatch listenerReady = new CountDownLatch(1);
        doAnswer(invocation -> {
            listenerReady.countDown();
            return null;
        }).when(polling).onListenerReconnected();
        PostgreSQLNotificationListener listener = listener(coordinator, polling);
        JobRun pending = pendingRun();

        listener.start();
        assertTrue(listenerReady.await(5, TimeUnit.SECONDS));
        notificationPublisher.publishRun(pending.id());
        assertTrue(executionStarted.await(5, TimeUnit.SECONDS));
        runCancellationService.requestCancellation(pending.id(), "remote cancellation", ignored -> { });

        assertTrue(cancellationObserved.await(5, TimeUnit.SECONDS));
        assertEquals(JobRunStatus.CANCELLED, awaitTerminal(pending.id()).status());
    }

    @Test
    void pollingDeliversCancellationWhenTheListenerIsDisabled() throws Exception {
        ActiveRunRegistry registry = new ActiveRunRegistry();
        CountDownLatch executionStarted = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        WorkerDispatchCoordinator coordinator = coordinator(registry, executionStarted, cancellationObserved);
        PollingFallback polling = polling(coordinator, Duration.ofMillis(20));
        JobRun pending = pendingRun();

        polling.start();
        assertTrue(executionStarted.await(5, TimeUnit.SECONDS));
        runCancellationService.requestCancellation(pending.id(), "polled cancellation", ignored -> { });

        assertTrue(cancellationObserved.await(5, TimeUnit.SECONDS));
        assertEquals(JobRunStatus.CANCELLED, awaitTerminal(pending.id()).status());
    }

    private WorkerDispatchCoordinator coordinator(ActiveRunRegistry registry,
                                                  CountDownLatch executionStarted,
                                                  CountDownLatch cancellationObserved) {
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
            executionStarted.countDown();
            while (!handle.cancellationContext().isCancellationRequested()) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Controlled execution interrupted", exception);
                }
            }
            cancellationObserved.countDown();
            runFinalizationService.markCancelled(run.id(), run.leaseToken(), 0, 0);
            return new JobRunOutcome(run.id(), JobRunStatus.CANCELLED, 0, 0);
        }).when(executionService).executeClaimedRun(any(), any());
        WorkerDispatchCoordinator coordinator = new WorkerDispatchCoordinator(
                runLeaseService, jobRunRepository, executionService, registry, heartbeatService,
                new WorkerRunIdentity(WORKER_IDENTITY), 1, Duration.ofMinutes(5), Duration.ofSeconds(2),
                metrics(), policy(), new WorkerAdmissionScheduler(), tracker(), 1_024);
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

    private PollingFallback polling(WorkerDispatchCoordinator coordinator, Duration interval) {
        PollingFallback polling = new PollingFallback(coordinator, jobRunRepository, runDispatchService,
                WORKER_IDENTITY, interval, 10, PollingFallback.newScheduler(), Duration.ofSeconds(2));
        pollers.add(polling);
        return polling;
    }

    private PostgreSQLNotificationListener listener(WorkerDispatchCoordinator coordinator,
                                                    PollingFallback polling) {
        PostgreSQLNotificationListener listener = new PostgreSQLNotificationListener(
            dataSource::getConnection,
                coordinator, polling, Duration.ofMillis(10), Duration.ofMillis(100),
                Duration.ofMillis(100), Duration.ofSeconds(2), duration -> Thread.sleep(duration.toMillis()),
                java.util.concurrent.Executors.newSingleThreadExecutor());
        listeners.add(listener);
        return listener;
    }

    private JobRun pendingRun() {
        return jobRunRepository.insertPendingNow(
                jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                        .withName("remote-cancel-" + UUID.randomUUID())
                    .withDefaultDatasourceReferences()
                        .build()).id(), null, 1);
    }

    private JobRun awaitTerminal(UUID runId) throws Exception {
        final JobRun[] result = new JobRun[1];
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            result[0] = jobRunRepository.findById(runId).orElseThrow();
            if (result[0].status().isTerminal()) {
                return result[0];
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Run did not reach a terminal state: " + runId);
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
}
