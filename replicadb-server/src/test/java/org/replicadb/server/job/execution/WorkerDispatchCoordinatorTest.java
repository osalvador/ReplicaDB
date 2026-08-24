package org.replicadb.server.job.execution;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;

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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
        coordinator = new WorkerDispatchCoordinator(
                runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry,
            heartbeatService, new WorkerRunIdentity(WORKER_IDENTITY), 1,
            LEASE_DURATION, Duration.ofSeconds(1));
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
                .thenReturn(Optional.of(run));
        when(jobRunStore.findById(run.id())).thenReturn(Optional.of(run));

        coordinator.signalEligibleWork();

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        await(() -> activeRunRegistry.find(run.id()).isEmpty());
        verify(jobRunStore).claimNextEligible(null, WORKER_IDENTITY, LEASE_DURATION);
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
        await(() -> activeRunRegistry.find(run.id()).isEmpty() && heartbeatStops.get() == 1);
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