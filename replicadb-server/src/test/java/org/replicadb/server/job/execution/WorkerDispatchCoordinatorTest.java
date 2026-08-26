package org.replicadb.server.job.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.config.WorkerRuntimeProperties;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.observability.WorkerBusySlotTracker;

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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerDispatchCoordinatorTest {

    private static final String WORKER_IDENTITY = "worker-a";
    private static final Duration LEASE_DURATION = Duration.ofSeconds(5);

    private JobRunStore jobRunStore;
    private RunLeaseService runLeaseService;
    private JobExecutionService jobExecutionService;
    private ActiveRunRegistry activeRunRegistry;
    private HeartbeatService heartbeatService;
    private AtomicInteger heartbeatStops;
    private HeartbeatHandle heartbeatHandle;
    private WorkerDispatchCoordinator coordinator;

    @BeforeEach
    void setUp() {
        jobRunStore = mock(JobRunStore.class);
        runLeaseService = new RunLeaseService(jobRunStore);
        jobExecutionService = mock(JobExecutionService.class);
        activeRunRegistry = new ActiveRunRegistry();
        heartbeatService = mock(HeartbeatService.class);
        heartbeatStops = new AtomicInteger();
        heartbeatHandle = new HeartbeatHandle(heartbeatStops::incrementAndGet);
        when(heartbeatService.start(any())).thenReturn(heartbeatHandle);
        coordinator = coordinator(1, immediatePolicy());
    }

    @AfterEach
    void stopCoordinator() {
        coordinator.shutdown(Duration.ofSeconds(1));
    }

    @Test
    void directedSignalClaimsAndExecutesTheRequestedRun() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch completed = stubSuccessfulExecution(run);
        when(jobRunStore.claimNextEligible(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(run));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));

        coordinator.signalRun(run.id());

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        await(() -> activeRunRegistry.find(run.id()).isEmpty() && heartbeatStops.get() == 1);
        verify(jobRunStore).claimNextEligible(run.id(), WORKER_IDENTITY, LEASE_DURATION);
        verify(jobExecutionService).executeClaimedRun(eq(run), any());
    }

    @Test
    void generalSignalClaimsTheNextEligibleRun() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch completed = stubSuccessfulExecution(run);
        when(jobRunStore.claimNextEligible(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.of(run), Optional.empty());
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));

        coordinator.signalEligibleWork();

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        await(() -> activeRunRegistry.find(run.id()).isEmpty());
        verify(jobRunStore, timeout(1_000).times(2))
            .claimNextEligible(null, WORKER_IDENTITY, LEASE_DURATION);
    }

    @Test
    void emptyClaimDoesNotInvokeSharedExecution() throws Exception {
        UUID runId = UUID.randomUUID();
        CountDownLatch claimAttempted = new CountDownLatch(1);
        when(jobRunStore.claimNextEligible(eq(runId), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenAnswer(invocation -> {
                    claimAttempted.countDown();
                    return Optional.empty();
                });

        coordinator.signalRun(runId);

        assertTrue(claimAttempted.await(2, TimeUnit.SECONDS));
        verify(jobExecutionService, never()).executeClaimedRun(any(), any());
    }

        @Test
        void emptyDirectedClaimPerformsExactlyOneImmediateFallback() throws Exception {
        UUID requestedRunId = UUID.randomUUID();
        JobRun fallbackRun = run(JobRunStatus.RUNNING);
        CountDownLatch completed = stubSuccessfulExecution(fallbackRun);
        when(jobRunStore.claimNextEligible(eq(requestedRunId), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.empty());
        when(jobRunStore.claimNextEligible(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.of(fallbackRun), Optional.empty());
        when(jobRunStore.findById(fallbackRun.id())).thenReturn(Optional.of(fallbackRun));

        coordinator.signalRun(requestedRunId);

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        verify(jobRunStore).claimNextEligible(requestedRunId, WORKER_IDENTITY, LEASE_DURATION);
        verify(jobRunStore, timeout(1_000).times(2))
            .claimNextEligible(null, WORKER_IDENTITY, LEASE_DURATION);
        verify(jobExecutionService).executeClaimedRun(eq(fallbackRun), any());
        }

        @Test
        void emptyDirectedAndFallbackClaimsDoNotChain() throws Exception {
        UUID requestedRunId = UUID.randomUUID();
            CountDownLatch directedClaimAttempted = new CountDownLatch(1);
        when(jobRunStore.claimNextEligible(eq(requestedRunId), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenAnswer(invocation -> {
                    directedClaimAttempted.countDown();
                    return Optional.empty();
                });
        when(jobRunStore.claimNextEligible(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.empty());

        coordinator.signalRun(requestedRunId);

            assertTrue(directedClaimAttempted.await(2, TimeUnit.SECONDS));
            verify(jobRunStore, timeout(1_000)).claimNextEligible(null, WORKER_IDENTITY, LEASE_DURATION);
        verify(jobRunStore).claimNextEligible(requestedRunId, WORKER_IDENTITY, LEASE_DURATION);
        verify(jobRunStore, times(2)).claimNextEligible(any(), eq(WORKER_IDENTITY), eq(LEASE_DURATION));
        verify(jobExecutionService, never()).executeClaimedRun(any(), any());
        }

    @Test
    void genericRefillUsesEachFreeSlotWithoutExceedingCapacity() throws Exception {
        coordinator.shutdown(Duration.ofSeconds(1));
        coordinator = coordinator(2, immediatePolicy());
        JobRun firstRun = run(JobRunStatus.RUNNING);
        JobRun secondRun = run(JobRunStatus.RUNNING);
        CountDownLatch executionsStarted = new CountDownLatch(2);
        CountDownLatch releaseExecutions = new CountDownLatch(1);
        when(jobRunStore.claimNextEligible(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(firstRun), Optional.of(secondRun));
        doAnswer(invocation -> {
            JobRun claimedRun = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(claimedRun, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            executionsStarted.countDown();
            releaseExecutions.await(2, TimeUnit.SECONDS);
            return new JobRunOutcome(claimedRun.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(any(), any());

        coordinator.requestGenericRefill("startup");

        assertTrue(executionsStarted.await(2, TimeUnit.SECONDS));
        assertEquals(0, coordinator.availableCapacity());
        verify(jobRunStore, times(2)).claimNextEligible(null, WORKER_IDENTITY, LEASE_DURATION);
        releaseExecutions.countDown();
        await(() -> coordinator.availableCapacity() == 2);
    }

    @Test
    void completedRunCreatesOneNextGenericRefill() throws Exception {
        when(jobRunStore.claimNextEligible(isNull(), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.of(run(JobRunStatus.RUNNING)), Optional.empty());
        CountDownLatch executions = new CountDownLatch(1);
        doAnswer(invocation -> {
            JobRun claimedRun = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(claimedRun, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            executions.countDown();
            return new JobRunOutcome(claimedRun.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(any(), any());

        coordinator.requestGenericRefill("startup");

        assertTrue(executions.await(2, TimeUnit.SECONDS));
        verify(jobRunStore, timeout(1_000).times(2))
            .claimNextEligible(null, WORKER_IDENTITY, LEASE_DURATION);
    }

        @Test
        void recordsBusySlotTimeAndTerminalOutcomeAtThePermitBoundary() throws Exception {
        coordinator.shutdown(Duration.ofSeconds(1));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ManagedRuntimeMetrics metrics = new ManagedRuntimeMetrics(registry);
        WorkerBusySlotTracker tracker = metrics.createWorkerBusySlotTracker(
            WORKER_IDENTITY, 1, System::nanoTime);
        coordinator = new WorkerDispatchCoordinator(
            runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
            new WorkerRunIdentity(WORKER_IDENTITY), 1, LEASE_DURATION, Duration.ofSeconds(1),
            metrics, immediatePolicy(), new WorkerAdmissionScheduler(), tracker, 1_024);
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch completed = new CountDownLatch(1);
        when(jobRunStore.claimNextEligible(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.of(run));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));
        doAnswer(invocation -> {
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            Thread.sleep(10);
            completed.countDown();
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 1, 10);
        }).when(jobExecutionService).executeClaimedRun(eq(run), any());

        coordinator.signalRun(run.id());

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        await(() -> coordinator.availableCapacity() == 1 && activeRunRegistry.find(run.id()).isEmpty());
        assertTrue(registry.get(ManagedRuntimeMetrics.BUSY_SLOT_SECONDS)
            .tag("worker_identity", WORKER_IDENTITY).gauge().value() > 0);
        assertTrue(registry.get(ManagedRuntimeMetrics.NORMALIZED_BUSY_SLOT_SECONDS)
            .tag("worker_identity", WORKER_IDENTITY).gauge().value() > 0);
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.COMPLETED_RUNS)
            .tag("worker_identity", WORKER_IDENTITY).tag("outcome", "succeeded").counter().count());
        }

    @Test
    void capacityRejectsASecondWakeupUntilTheFirstRunCompletes() throws Exception {
        JobRun firstRun = run(JobRunStatus.RUNNING);
        JobRun secondRun = run(JobRunStatus.RUNNING);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondExecuted = new CountDownLatch(1);
        when(jobRunStore.claimNextEligible(eq(firstRun.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(firstRun));
        when(jobRunStore.claimNextEligible(eq(secondRun.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(secondRun));
        when(jobRunStore.findById(firstRun.id())).thenReturn(Optional.of(firstRun));
        when(jobRunStore.findById(secondRun.id())).thenReturn(Optional.of(secondRun));
        doAnswer(invocation -> {
            JobRun run = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            if (run.id().equals(firstRun.id())) {
                firstEntered.countDown();
                releaseFirst.await(2, TimeUnit.SECONDS);
            } else {
                secondExecuted.countDown();
            }
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(any(), any());

        coordinator.signalRun(firstRun.id());
        assertTrue(firstEntered.await(2, TimeUnit.SECONDS));
        coordinator.signalRun(secondRun.id());

        assertFalse(secondExecuted.await(100, TimeUnit.MILLISECONDS));
        releaseFirst.countDown();
        await(() -> activeRunRegistry.find(firstRun.id()).isEmpty());

        coordinator.signalRun(secondRun.id());
        assertTrue(secondExecuted.await(2, TimeUnit.SECONDS));
        verify(jobRunStore, times(1)).claimNextEligible(secondRun.id(), WORKER_IDENTITY, LEASE_DURATION);
    }

    @Test
    void durableCancellationIsDeliveredBeforeHeartbeatStarts() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        JobRun cancellationRequested = copyWithStatus(run, JobRunStatus.CANCEL_REQUESTED);
        when(jobRunStore.claimNextEligible(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(run));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(cancellationRequested));
        AtomicBoolean cancelledBeforeHeartbeat = new AtomicBoolean();
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            cancelledBeforeHeartbeat.set(handle.cancellationContext().isCancellationRequested());
            completed.countDown();
            return new JobRunOutcome(run.id(), JobRunStatus.CANCELLED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(eq(run), any());

        coordinator.signalRun(run.id());

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertTrue(cancelledBeforeHeartbeat.get());
        verify(heartbeatService).start(any());
    }

    @Test
    void coreFailureStillStopsHeartbeatAndRemovesTheHandle() throws Exception {
        JobRun run = run(JobRunStatus.RUNNING);
        CountDownLatch failed = new CountDownLatch(1);
        when(jobRunStore.claimNextEligible(eq(run.id()), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
                .thenReturn(Optional.of(run));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));
        doAnswer(invocation -> {
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            failed.countDown();
            throw new IllegalStateException("core failure");
        }).when(jobExecutionService).executeClaimedRun(eq(run), any());

        coordinator.signalRun(run.id());

        assertTrue(failed.await(2, TimeUnit.SECONDS));
        await(() -> activeRunRegistry.find(run.id()).isEmpty()
            && heartbeatStops.get() == 1
            && coordinator.availableCapacity() == 1);
        }

        @Test
        void delayedAdmissionDoesNotConsumeCapacityBeforeClaim() throws Exception {
        coordinator.shutdown(Duration.ofSeconds(1));
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        configuration.setJitterMax(Duration.ofMillis(100));
        WorkerAdmissionPolicy delayedPolicy = new WorkerAdmissionPolicy(configuration,
            System::nanoTime, () -> 1.0);
        coordinator = coordinator(1, delayedPolicy);
        UUID runId = UUID.randomUUID();
        when(jobRunStore.claimNextEligible(eq(runId), eq(WORKER_IDENTITY), eq(LEASE_DURATION)))
            .thenReturn(Optional.empty());

        coordinator.signalRun(runId);

        assertTrue(coordinator.availableCapacity() == 1);
        verify(jobRunStore, never()).claimNextEligible(eq(runId), eq(WORKER_IDENTITY), eq(LEASE_DURATION));
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
        verify(jobRunStore, never()).claimNextEligible(any(), any(), any());
    }

    @Test
    void blankIdentityIsGeneratedOncePerWorker() {
        WorkerRunIdentity first = WorkerRunIdentity.resolve("");
        WorkerRunIdentity second = WorkerRunIdentity.resolve(null);

        assertTrue(first.value().startsWith("worker-"));
        assertTrue(second.value().startsWith("worker-"));
        assertNotEquals(first.value(), second.value());
    }

    private CountDownLatch stubSuccessfulExecution(JobRun run) {
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            activeRunRegistry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            completed.countDown();
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(jobExecutionService).executeClaimedRun(eq(run), any());
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
                null, null, null, null, null, now, LeaseToken.generate());
    }

    private static JobRun copyWithStatus(JobRun run, JobRunStatus status) {
        return new JobRun(run.id(), run.jobDefinitionId(), run.previousRunId(), status, run.attempt(),
                run.executorIdentity(), run.leaseUntil(), run.heartbeatAt(), run.createdAt(),
                run.startedAt(), run.finishedAt(), run.rowsProcessed(), run.durationMillis(),
                run.committedWatermark(), run.errorMessage(), run.cancellationWarning(),
                run.availableAt(), run.leaseToken());
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