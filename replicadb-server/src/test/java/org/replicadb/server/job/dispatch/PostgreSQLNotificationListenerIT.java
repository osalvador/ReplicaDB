package org.replicadb.server.job.dispatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGNotification;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.execution.ActiveRunRegistry;
import org.replicadb.server.job.execution.HeartbeatHandle;
import org.replicadb.server.job.execution.HeartbeatService;
import org.replicadb.server.job.execution.JobExecutionService;
import org.replicadb.server.job.execution.JobRunOutcome;
import org.replicadb.server.job.execution.RunExecutionHandle;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.execution.WorkerRunIdentity;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.PostgresNotificationPublisher;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class PostgreSQLNotificationListenerIT {

    private static final String WORKER_IDENTITY = "listener-it-worker";

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private RunLeaseService runLeaseService;

    @Autowired
    private RunDispatchService runDispatchService;

    @Autowired
    private PostgresNotificationPublisher notificationPublisher;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    private final List<PostgreSQLNotificationListener> listeners = new CopyOnWriteArrayList<>();
    private final List<PollingFallback> pollers = new CopyOnWriteArrayList<>();
    private final List<WorkerDispatchCoordinator> coordinators = new CopyOnWriteArrayList<>();
    private final List<HeartbeatService> heartbeats = new CopyOnWriteArrayList<>();
    private final List<Connection> listenerConnections = new CopyOnWriteArrayList<>();

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
    }

    @AfterEach
    void stopRuntime() {
        listeners.forEach(PostgreSQLNotificationListener::stop);
        pollers.forEach(PollingFallback::stop);
        coordinators.forEach(coordinator -> coordinator.shutdown(Duration.ofSeconds(2)));
        heartbeats.forEach(HeartbeatService::shutdown);
        listeners.clear();
        pollers.clear();
        coordinators.clear();
        heartbeats.clear();
        listenerConnections.clear();
    }

    @Test
    void receivesRunAndCancellationNotificationsOnTheDedicatedListener() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        UUID runId = UUID.randomUUID();
        UUID cancellationId = UUID.randomUUID();
        CountDownLatch routed = new CountDownLatch(2);
        doAnswer(invocation -> {
            routed.countDown();
            return null;
        }).when(coordinator).signalRun(runId);
        when(coordinator.signalCancellation(cancellationId)).thenAnswer(invocation -> {
            routed.countDown();
            return true;
        });
        AtomicInteger reconnects = reconnectCounter(polling);
        PostgreSQLNotificationListener listener = listener(coordinator, polling,
                Duration.ofMillis(10), Duration.ofMillis(100));

        listener.start();
        await(() -> reconnects.get() >= 1);
        notificationPublisher.publishRun(runId);
        notificationPublisher.publishCancellation(cancellationId);

        assertTrue(routed.await(5, TimeUnit.SECONDS));
        verify(coordinator).signalRun(runId);
        verify(coordinator).signalCancellation(cancellationId);
        assertTrue(listenerConnections.size() >= 1);
    }

    @Test
    void duplicateNotificationsProduceAtMostOneDatabaseClaim() throws Exception {
        JobRun pending = pendingRun();
        JobExecutionService executionService = mock(JobExecutionService.class);
        ActiveRunRegistry registry = new ActiveRunRegistry();
        WorkerDispatchCoordinator coordinator = coordinator(executionService, registry);
        PollingFallback polling = mock(PollingFallback.class);
        CountDownLatch executionStarted = new CountDownLatch(1);
        CountDownLatch releaseExecution = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        doAnswer(invocation -> {
            JobRun run = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            registry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            executions.incrementAndGet();
            executionStarted.countDown();
            releaseExecution.await(5, TimeUnit.SECONDS);
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(executionService).executeClaimedRun(any(), any());
        AtomicInteger reconnects = reconnectCounter(polling);
        PostgreSQLNotificationListener listener = listener(coordinator, polling,
                Duration.ofMillis(10), Duration.ofMillis(100));

        listener.start();
        await(() -> reconnects.get() >= 1);
        notificationPublisher.publishRun(pending.id());
        notificationPublisher.publishRun(pending.id());

        assertTrue(executionStarted.await(5, TimeUnit.SECONDS));
        assertEquals(1, executions.get());
        assertEquals(JobRunStatus.RUNNING, jobRunRepository.findById(pending.id()).orElseThrow().status());
        releaseExecution.countDown();
    }

    @Test
    void pollingRecoversAWorkNotificationPublishedWhileListenerWasDisconnected() throws Exception {
        JobExecutionService executionService = mock(JobExecutionService.class);
        ActiveRunRegistry registry = new ActiveRunRegistry();
        WorkerDispatchCoordinator coordinator = coordinator(executionService, registry);
        PollingFallback polling = poller(coordinator, Duration.ofHours(1));
        polling.start();
        JobRun pending = pendingRun();

        notificationPublisher.publishRun(pending.id());
        polling.scanNow();

        assertEquals(JobRunStatus.RUNNING, awaitRun(pending.id()).status());
    }

    @Test
    void reconnectResubscribesAndUsesAConnectionSeparateFromRepositoryWork() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        AtomicInteger reconnects = reconnectCounter(polling);
        PostgreSQLNotificationListener listener = listener(coordinator, polling,
                Duration.ofMillis(10), Duration.ofMillis(100));

        listener.start();
        await(() -> reconnects.get() >= 1);
        Connection firstListenerConnection = listenerConnections.get(0);
        try (Connection repositoryConnection = dataSource.getConnection()) {
            assertNotSame(firstListenerConnection, repositoryConnection);
        }
        terminateBackend(firstListenerConnection);

        await(() -> reconnects.get() >= 2);
        assertTrue(listenerConnections.size() >= 2);
        verify(polling, times(2)).onListenerReconnected();
    }

    @Test
    void listenerReconnectDoesNotStopAnIndependentHeartbeat() throws Exception {
        JobRun pending = pendingRun();
        JobRun claimed = runLeaseService.claimRequested(
                pending.id(), WORKER_IDENTITY, Duration.ofSeconds(5)).orElseThrow();
        HeartbeatService heartbeat = new HeartbeatService(runLeaseService, Duration.ofMillis(20),
                Duration.ofSeconds(5), Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(2));
        heartbeats.add(heartbeat);
        RunExecutionHandle handle = new RunExecutionHandle(claimed, options());
        HeartbeatHandle heartbeatHandle = heartbeat.start(handle);
        await(() -> jobRunRepository.findById(claimed.id()).orElseThrow()
                .heartbeatAt().isAfter(claimed.heartbeatAt()));
        JobRun beforeDisconnect = jobRunRepository.findById(claimed.id()).orElseThrow();
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        AtomicInteger reconnects = reconnectCounter(polling);
        PostgreSQLNotificationListener listener = listener(coordinator, polling,
                Duration.ofMillis(10), Duration.ofMillis(100));

        listener.start();
        await(() -> reconnects.get() >= 1);
        terminateBackend(listenerConnections.get(0));
        await(() -> reconnects.get() >= 2);
        await(() -> jobRunRepository.findById(claimed.id()).orElseThrow()
                .heartbeatAt().isAfter(beforeDisconnect.heartbeatAt()));

        assertFalse(heartbeatHandle.isStopped());
        assertTrue(jobRunRepository.findById(claimed.id()).orElseThrow()
                .leaseUntil().isAfter(beforeDisconnect.leaseUntil()));
    }

    private AtomicInteger reconnectCounter(PollingFallback polling) {
        AtomicInteger reconnects = new AtomicInteger();
        doAnswer(invocation -> {
            reconnects.incrementAndGet();
            return null;
        }).when(polling).onListenerReconnected();
        return reconnects;
    }

    private PostgreSQLNotificationListener listener(WorkerDispatchCoordinator coordinator,
                                                    PollingFallback polling,
                                                    Duration initialDelay,
                                                    Duration maxDelay) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PostgreSQLNotificationListener listener = new PostgreSQLNotificationListener(
                () -> {
                    Connection connection = dataSource.getConnection();
                    listenerConnections.add(connection);
                    return connection;
                }, coordinator, polling, initialDelay, maxDelay, Duration.ofMillis(100),
                Duration.ofSeconds(2), duration -> Thread.sleep(duration.toMillis()), executor);
        listeners.add(listener);
        return listener;
    }

    private WorkerDispatchCoordinator coordinator(JobExecutionService executionService,
                                                  ActiveRunRegistry registry) {
        HeartbeatService heartbeatService = mock(HeartbeatService.class);
        when(heartbeatService.start(any())).thenReturn(mock(HeartbeatHandle.class));
        WorkerDispatchCoordinator coordinator = new WorkerDispatchCoordinator(
                runLeaseService, jobRunRepository, executionService, registry, heartbeatService,
                new WorkerRunIdentity(WORKER_IDENTITY), 1, Duration.ofMinutes(5), Duration.ofSeconds(2));
        coordinators.add(coordinator);
        return coordinator;
    }

    private PollingFallback poller(WorkerDispatchCoordinator coordinator, Duration interval) {
        PollingFallback polling = new PollingFallback(coordinator, jobRunRepository, runDispatchService,
                WORKER_IDENTITY, interval, 10, PollingFallback.newScheduler(), Duration.ofSeconds(2));
        pollers.add(polling);
        return polling;
    }

    private JobRun pendingRun() {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("listener-job-" + UUID.randomUUID())
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

    private void terminateBackend(Connection connection) throws Exception {
        int backendPid;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT pg_backend_pid()")) {
            resultSet.next();
            backendPid = resultSet.getInt(1);
        }
        try (Connection killer = dataSource.getConnection();
             PreparedStatement statement = killer.prepareStatement("SELECT pg_terminate_backend(?)")) {
            statement.setInt(1, backendPid);
            statement.execute();
        }
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
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