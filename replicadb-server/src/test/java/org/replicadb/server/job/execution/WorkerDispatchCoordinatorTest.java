package org.replicadb.server.job.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.config.WorkerRuntimeProperties;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.observability.WorkerBusySlotTracker;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerDispatchCoordinatorTest {

    private static final String WORKER_IDENTITY = "worker-a";
    private static final Duration LEASE_DURATION = Duration.ofSeconds(5);
    private static final UUID SOURCE_DATASOURCE_ID = UUID.fromString(
            "00000000-0000-0000-0000-000000000051");
    private static final UUID SINK_DATASOURCE_ID = UUID.fromString(
            "00000000-0000-0000-0000-000000000052");

    private JobRunStore jobRunStore;
    private RunLeaseService runLeaseService;
    private JobExecutionService jobExecutionService;
    private ActiveRunRegistry activeRunRegistry;
    private HeartbeatService heartbeatService;
    private AtomicInteger heartbeatStops;
    private WorkerDispatchCoordinator coordinator;

    @BeforeEach
    void setUp() {
        jobRunStore = mock(JobRunStore.class);
        runLeaseService = new RunLeaseService(jobRunStore);
        jobExecutionService = mock(JobExecutionService.class);
        activeRunRegistry = new ActiveRunRegistry();
        heartbeatService = mock(HeartbeatService.class);
        heartbeatStops = new AtomicInteger();
        HeartbeatHandle heartbeatHandle = new HeartbeatHandle(heartbeatStops::incrementAndGet);
        when(heartbeatService.start(any())).thenReturn(heartbeatHandle);
        coordinator = coordinator(1, immediatePolicy());
    }

    @AfterEach
    void stopCoordinator() {
        coordinator.shutdown(Duration.ofSeconds(1));
    }

    @Test
    void directedSignalClaimsPreparationAndExecutesIt() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch completed = stubSuccessfulExecution();
        when(jobRunStore.claimAndPrepare(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(preparation(run)));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));

        coordinator.signalRun(run.id());

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        await(() -> activeRunRegistry.find(run.id()).isEmpty() && heartbeatStops.get() == 1);
        verify(jobRunStore).claimAndPrepare(run.id(), WORKER_IDENTITY, LEASE_DURATION);
        verify(jobExecutionService).executeClaimedRun(any(ClaimedRunPreparation.class), any());
    }

    @Test
    void genericSignalClaimsTheNextPreparation() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch completed = stubSuccessfulExecution();
        CountDownLatch refillClaimed = new CountDownLatch(1);
        AtomicInteger claimCount = new AtomicInteger();
        when(jobRunStore.claimAndPrepare(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenAnswer(invocation -> {
                    if (claimCount.getAndIncrement() == 0) {
                        return Optional.of(preparation(run));
                    }
                    refillClaimed.countDown();
                    return Optional.empty();
                });
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));

        coordinator.signalEligibleWork();

        assertTrue(completed.await(10, TimeUnit.SECONDS));
        assertTrue(refillClaimed.await(10, TimeUnit.SECONDS));
        verify(jobRunStore, timeout(1_000).times(2))
                .claimAndPrepare(null, WORKER_IDENTITY, LEASE_DURATION);
    }

    @Test
    void emptyDirectedClaimFallsBackOnceWithoutExecutingAnUnclaimedRun() throws Exception {
        UUID requestedRunId = UUID.randomUUID();
        JobRun fallback = run(JobRunStatus.RUNNING);
        CountDownLatch completed = stubSuccessfulExecution();
        when(jobRunStore.claimAndPrepare(eq(requestedRunId), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.empty());
        when(jobRunStore.claimAndPrepare(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(preparation(fallback)), Optional.empty());
        when(jobRunStore.findById(fallback.id())).thenReturn(Optional.of(fallback));

        coordinator.signalRun(requestedRunId);

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        verify(jobRunStore).claimAndPrepare(requestedRunId, WORKER_IDENTITY, LEASE_DURATION);
        verify(jobRunStore, timeout(1_000).times(2))
                .claimAndPrepare(null, WORKER_IDENTITY, LEASE_DURATION);
        verify(jobExecutionService).executeClaimedRun(any(ClaimedRunPreparation.class), any());
    }

    @Test
    void durableCancellationIsDeliveredBeforeHeartbeatStarts() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        JobRun cancellationRequested = copyWithStatus(run, JobRunStatus.CANCEL_REQUESTED);
        when(jobRunStore.claimAndPrepare(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(preparation(run)));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(cancellationRequested));
        AtomicBoolean cancelledBeforeHeartbeat = new AtomicBoolean();
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            ClaimedRunPreparation claimed = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(claimed.run(), options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            cancelledBeforeHeartbeat.set(handle.cancellationContext().isCancellationRequested());
            completed.countDown();
            return new JobRunOutcome(claimed.run().id(), JobRunStatus.CANCELLED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(any(ClaimedRunPreparation.class), any());

        coordinator.signalRun(run.id());

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertTrue(cancelledBeforeHeartbeat.get());
        verify(heartbeatService).start(any());
    }

    @Test
    void coreFailureStillStopsHeartbeatAndReleasesCapacity() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch failed = new CountDownLatch(1);
        when(jobRunStore.claimAndPrepare(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(preparation(run)));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));
        doAnswer(invocation -> {
            ClaimedRunPreparation claimed = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(claimed.run(), options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            failed.countDown();
            throw new IllegalStateException("core failure");
        }).when(jobExecutionService).executeClaimedRun(any(ClaimedRunPreparation.class), any());

        coordinator.signalRun(run.id());

        assertTrue(failed.await(2, TimeUnit.SECONDS));
        await(() -> activeRunRegistry.find(run.id()).isEmpty()
                && heartbeatStops.get() == 1 && coordinator.availableCapacity() == 1);
    }

    @Test
    void shutdownStopsIntakeAndCancelsActiveLocalHandles() {
        JobRun run = run(JobRunStatus.RUNNING);
        RunExecutionHandle handle = new RunExecutionHandle(run, options());
        activeRunRegistry.register(handle);

        coordinator.stopAccepting();
        coordinator.signalRun(run.id());
        coordinator.shutdown(Duration.ofSeconds(1));

        assertTrue(handle.cancellationContext().isCancellationRequested());
        assertTrue(coordinator.isShutdown());
        verify(jobRunStore, never()).claimAndPrepare(any(), any(), any());
    }

    @Test
    void blankIdentityIsGeneratedOncePerWorker() {
        WorkerRunIdentity first = WorkerRunIdentity.resolve("");
        WorkerRunIdentity second = WorkerRunIdentity.resolve(null);

        assertTrue(first.value().startsWith("worker-"));
        assertTrue(second.value().startsWith("worker-"));
        assertNotEquals(first.value(), second.value());
    }

    private CountDownLatch stubSuccessfulExecution() {
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            ClaimedRunPreparation claimed = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(claimed.run(), options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            completed.countDown();
            return new JobRunOutcome(claimed.run().id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(any(ClaimedRunPreparation.class), any());
        return completed;
    }

    private WorkerDispatchCoordinator coordinator(int maxConcurrentRuns, WorkerAdmissionPolicy policy) {
        ManagedRuntimeMetrics metrics = ManagedRuntimeMetrics.noop();
        return new WorkerDispatchCoordinator(
                runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
                new WorkerRunIdentity(WORKER_IDENTITY), maxConcurrentRuns, LEASE_DURATION,
                Duration.ofSeconds(1), metrics, policy, new WorkerAdmissionScheduler(),
                metrics.createWorkerBusySlotTracker(WORKER_IDENTITY, maxConcurrentRuns, System::nanoTime));
    }

    private static WorkerAdmissionPolicy immediatePolicy() {
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        configuration.setJitterMax(Duration.ZERO);
        configuration.setGenericCooldown(Duration.ZERO);
        configuration.getAdaptiveBackoff().setEnabled(false);
        return new WorkerAdmissionPolicy(configuration, System::nanoTime, () -> 0.0);
    }

    private static JobRun run(JobRunStatus status) {
        Instant now = Instant.now();
        return new JobRun(UUID.randomUUID(), UUID.randomUUID(), null, status, 1,
                WORKER_IDENTITY, now.plusSeconds(300), now, now, now, null,
                null, null, null, null, null, now, LeaseToken.generate(),
                SOURCE_DATASOURCE_ID, SINK_DATASOURCE_ID, now);
    }

    private static JobRun copyWithStatus(JobRun run, JobRunStatus status) {
        return new JobRun(run.id(), run.jobDefinitionId(), run.previousRunId(), status, run.attempt(),
                run.executorIdentity(), run.leaseUntil(), run.heartbeatAt(), run.createdAt(),
                run.startedAt(), run.finishedAt(), run.rowsProcessed(), run.durationMillis(),
                run.committedWatermark(), run.errorMessage(), run.cancellationWarning(),
                run.availableAt(), run.leaseToken(), run.resolvedSourceDatasourceId(),
                run.resolvedSinkDatasourceId(), run.datasourcesResolvedAt());
    }

    private static ClaimedRunPreparation preparation(JobRun run) {
        JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
                .withId(run.jobDefinitionId())
                .withSourceDatasourceId(SOURCE_DATASOURCE_ID)
                .withSinkDatasourceId(SINK_DATASOURCE_ID)
                .build();
        return new ClaimedRunPreparation(run, definition,
                datasource(SOURCE_DATASOURCE_ID, "source"), datasource(SINK_DATASOURCE_ID, "sink"));
    }

    private static ManagedDataSource datasource(UUID id, String name) {
        return new ManagedDataSource(id, name, ConnectorType.POSTGRES,
                "jdbc:postgresql://[REDACTED]/db", Map.of(), new byte[]{1},
                1, "AES-256-GCM", "test", null, null);
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline && !check.completed()) {
            Thread.sleep(5);
        }
        assertTrue(check.completed());
    }

    @FunctionalInterface
    private interface Check {
        boolean completed();
    }
}
