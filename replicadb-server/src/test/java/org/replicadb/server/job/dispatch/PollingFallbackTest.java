package org.replicadb.server.job.dispatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.job.application.RunDispatchResult;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PollingFallbackTest {

    private static final String WORKER_IDENTITY = "polling-worker";

    private JobRunStore jobRunStore;
    private RunDispatchService runDispatchService;
    private WorkerDispatchCoordinator workerCoordinator;
    private ScheduledExecutorService scheduler;
    private PollingFallback pollingFallback;

    @BeforeEach
    void setUp() {
        jobRunStore = mock(JobRunStore.class);
        runDispatchService = mock(RunDispatchService.class);
        workerCoordinator = mock(WorkerDispatchCoordinator.class);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        when(jobRunStore.findCancellationRequestedRunIds(eq(WORKER_IDENTITY), anyInt())).thenReturn(List.of());
        when(jobRunStore.findExpiredRunIds(anyInt())).thenReturn(List.of());
        pollingFallback = new PollingFallback(workerCoordinator, jobRunStore, runDispatchService,
                WORKER_IDENTITY, Duration.ofHours(1), 2, scheduler, Duration.ofSeconds(1));
    }

    @AfterEach
    void stopPolling() {
        pollingFallback.stop();
        scheduler.shutdownNow();
    }

    @Test
    void startupAndPeriodicScansRunImmediatelyAndAtTheConfiguredInterval() throws Exception {
        pollingFallback = new PollingFallback(workerCoordinator, jobRunStore, runDispatchService,
                WORKER_IDENTITY, Duration.ofMillis(20), 2, scheduler, Duration.ofSeconds(1));

        pollingFallback.start();

        verify(workerCoordinator, timeout(1_000).atLeast(2)).signalEligibleWork();
        assertTrue(pollingFallback.isRunning());
    }

    @Test
    void listenerReconnectSchedulesAnotherScan() throws Exception {
        pollingFallback.start();
        clearInvocations(workerCoordinator);

        pollingFallback.onListenerReconnected();

        verify(workerCoordinator, timeout(1_000)).signalEligibleWork();
    }

    @Test
    void overlappingScansAreSuppressed() throws Exception {
        pollingFallback.start();
        clearInvocations(workerCoordinator, jobRunStore);
        CountDownLatch scanEntered = new CountDownLatch(1);
        CountDownLatch releaseScan = new CountDownLatch(1);
        when(jobRunStore.findCancellationRequestedRunIds(eq(WORKER_IDENTITY), eq(2)))
                .thenAnswer(invocation -> {
                    scanEntered.countDown();
                    releaseScan.await(2, TimeUnit.SECONDS);
                    return List.of();
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> runningScan = executor.submit(pollingFallback::scanNow);
            assertTrue(scanEntered.await(2, TimeUnit.SECONDS));

            pollingFallback.scanNow();

            verify(jobRunStore, times(1))
                    .findCancellationRequestedRunIds(WORKER_IDENTITY, 2);
            releaseScan.countDown();
            runningScan.get(2, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void boundedCancellationAndExpiryScansSignalOnlyReturnedWork() throws Exception {
        UUID cancellationOne = UUID.randomUUID();
        UUID cancellationTwo = UUID.randomUUID();
        UUID expiredOne = UUID.randomUUID();
        UUID expiredTwo = UUID.randomUUID();
        UUID replacement = UUID.randomUUID();
        when(jobRunStore.findCancellationRequestedRunIds(WORKER_IDENTITY, 2))
                .thenReturn(List.of(cancellationOne, cancellationTwo));
        when(jobRunStore.findExpiredRunIds(2)).thenReturn(List.of(expiredOne, expiredTwo));
        when(runDispatchService.recoverExpiredRun(expiredOne)).thenReturn(
                new RunDispatchResult(Optional.of(mockRun(replacement)),
                        RunDispatchResult.Outcome.RECOVERY_REPLACEMENT));
        when(runDispatchService.recoverExpiredRun(expiredTwo)).thenReturn(
                new RunDispatchResult(Optional.empty(), RunDispatchResult.Outcome.RECOVERY_NOOP));

        pollingFallback.start();

        verify(workerCoordinator).signalEligibleWork();
        verify(workerCoordinator).signalCancellation(cancellationOne);
        verify(workerCoordinator).signalCancellation(cancellationTwo);
        verify(runDispatchService).recoverExpiredRun(expiredOne);
        verify(runDispatchService).recoverExpiredRun(expiredTwo);
        verify(workerCoordinator).signalRun(replacement);
        verify(jobRunStore).findCancellationRequestedRunIds(WORKER_IDENTITY, 2);
        verify(jobRunStore).findExpiredRunIds(2);
    }

    @Test
    void missedCancellationWithNoLocalHandleIsStillScanned() {
        UUID runId = UUID.randomUUID();
        when(jobRunStore.findCancellationRequestedRunIds(WORKER_IDENTITY, 2)).thenReturn(List.of(runId));
        when(workerCoordinator.signalCancellation(runId)).thenReturn(false);

        pollingFallback.start();

        verify(workerCoordinator).signalCancellation(runId);
        verify(runDispatchService, never()).recoverExpiredRun(any(UUID.class));
        assertTrue(pollingFallback.isRunning());
    }

    private static org.replicadb.server.job.domain.JobRun mockRun(UUID runId) {
        return new org.replicadb.server.job.domain.JobRun(runId, UUID.randomUUID(), null,
                org.replicadb.server.job.domain.JobRunStatus.PENDING, 1, null, null, null,
                java.time.Instant.now(), null, null, null, null, null, null, null,
                java.time.Instant.now(), org.replicadb.server.job.domain.LeaseToken.generate());
    }
}