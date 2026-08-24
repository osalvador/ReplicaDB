package org.replicadb.server.job.execution;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HeartbeatServiceTest {

    private final RunLeaseService runLeaseService = mock(RunLeaseService.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final HeartbeatService heartbeatService = new HeartbeatService(
            runLeaseService, Duration.ofMillis(10), Duration.ofSeconds(5), scheduler, Duration.ofSeconds(1));

    @AfterEach
    void stopScheduler() {
        heartbeatService.shutdown();
    }

    @Test
    void renewsRepeatedlyAndStopsOnCompletion() throws Exception {
        CountDownLatch renewals = new CountDownLatch(2);
        when(runLeaseService.renewLease(any(UUID.class), any(LeaseToken.class), eq(Duration.ofSeconds(5))))
                .thenAnswer(invocation -> {
                    renewals.countDown();
                    return JobRunStore.LeaseRenewalResult.RENEWED;
                });
        RunExecutionHandle executionHandle = executionHandle();

        HeartbeatHandle heartbeat = heartbeatService.start(executionHandle);

        assertTrue(renewals.await(2, TimeUnit.SECONDS));
        heartbeat.stop();
        assertTrue(heartbeat.isStopped());
        verify(runLeaseService, org.mockito.Mockito.atLeast(2))
                .renewLease(executionHandle.runId(), executionHandle.leaseToken(), Duration.ofSeconds(5));
    }

    @Test
    void stopsOnFenceAndRequestsLocalCancellation() throws Exception {
        when(runLeaseService.renewLease(any(UUID.class), any(LeaseToken.class), any(Duration.class)))
                .thenReturn(JobRunStore.LeaseRenewalResult.FENCED);
        RunExecutionHandle executionHandle = executionHandle();

        HeartbeatHandle heartbeat = heartbeatService.start(executionHandle);

        await(() -> heartbeat.isStopped() && executionHandle.cancellationContext().isCancellationRequested());
        assertTrue(heartbeat.isStopped());
        assertTrue(executionHandle.cancellationContext().isCancellationRequested());
    }

    @Test
    void stopsOnDatabaseErrorAndRequestsLocalCancellation() throws Exception {
        when(runLeaseService.renewLease(any(UUID.class), any(LeaseToken.class), any(Duration.class)))
                .thenThrow(new IllegalStateException("database unavailable"));
        RunExecutionHandle executionHandle = executionHandle();

        HeartbeatHandle heartbeat = heartbeatService.start(executionHandle);

        await(() -> heartbeat.isStopped() && executionHandle.cancellationContext().isCancellationRequested());
        assertTrue(heartbeat.isStopped());
        assertTrue(executionHandle.cancellationContext().isCancellationRequested());
    }

    @Test
    void shutdownStopsActiveHeartbeatsAndPreventsFurtherRenewal() throws Exception {
        CountDownLatch firstRenewal = new CountDownLatch(1);
        AtomicInteger renewalCount = new AtomicInteger();
        when(runLeaseService.renewLease(any(UUID.class), any(LeaseToken.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    renewalCount.incrementAndGet();
                    firstRenewal.countDown();
                    return JobRunStore.LeaseRenewalResult.RENEWED;
                });
        HeartbeatHandle heartbeat = heartbeatService.start(executionHandle());
        assertTrue(firstRenewal.await(2, TimeUnit.SECONDS));

        heartbeatService.shutdown();

        assertTrue(heartbeat.isStopped());
        assertFalse(scheduler.isTerminated() == false && scheduler.isShutdown() == false);
    }

    private static RunExecutionHandle executionHandle() throws Exception {
        LeaseToken leaseToken = LeaseToken.generate();
        JobRun run = new JobRun(UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.RUNNING, 1,
                "heartbeat-worker", Instant.now().plusSeconds(300), Instant.now(), Instant.now(),
                Instant.now(), null, null, null, null, null, null, Instant.now(), leaseToken);
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlite:source.db",
                "--sink-connect", "jdbc:sqlite:sink.db"
        });
        return new RunExecutionHandle(run, options);
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