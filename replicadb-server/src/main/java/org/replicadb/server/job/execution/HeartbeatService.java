package org.replicadb.server.job.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatService implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(HeartbeatService.class);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final RunLeaseService runLeaseService;
    private final Duration heartbeatInterval;
    private final Duration leaseDuration;
    private final Duration shutdownTimeout;
    private final ScheduledExecutorService scheduler;
    private final Set<HeartbeatHandle> activeHeartbeats = ConcurrentHashMap.newKeySet();

    public HeartbeatService(RunLeaseService runLeaseService,
                            Duration heartbeatInterval,
                            Duration leaseDuration) {
        this(runLeaseService, heartbeatInterval, leaseDuration,
                Executors.newSingleThreadScheduledExecutor(new HeartbeatThreadFactory()),
                DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public HeartbeatService(RunLeaseService runLeaseService,
                            Duration heartbeatInterval,
                            Duration leaseDuration,
                            ScheduledExecutorService scheduler,
                            Duration shutdownTimeout) {
        this.runLeaseService = Objects.requireNonNull(runLeaseService, "runLeaseService must not be null");
        this.heartbeatInterval = positive(heartbeatInterval, "heartbeatInterval");
        this.leaseDuration = positive(leaseDuration, "leaseDuration");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.shutdownTimeout = positive(shutdownTimeout, "shutdownTimeout");
    }

    public HeartbeatHandle start(RunExecutionHandle executionHandle) {
        Objects.requireNonNull(executionHandle, "executionHandle must not be null");
        if (scheduler.isShutdown()) {
            throw new IllegalStateException("Heartbeat service is shut down");
        }

        AtomicReference<ScheduledFuture<?>> scheduledFuture = new AtomicReference<>();
        AtomicReference<HeartbeatHandle> heartbeatReference = new AtomicReference<>();
        HeartbeatHandle heartbeat = new HeartbeatHandle(() -> {
            HeartbeatHandle current = heartbeatReference.get();
            if (current != null) {
                activeHeartbeats.remove(current);
            }
            ScheduledFuture<?> future = scheduledFuture.get();
            if (future != null) {
                future.cancel(false);
            }
        });
        heartbeatReference.set(heartbeat);
        activeHeartbeats.add(heartbeat);
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                () -> renew(executionHandle, heartbeat),
                0,
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        scheduledFuture.set(future);
        if (heartbeat.isStopped()) {
            future.cancel(false);
        }
        return heartbeat;
    }

    public void shutdown() {
        for (HeartbeatHandle heartbeat : activeHeartbeats) {
            heartbeat.stop();
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warn("Heartbeat scheduler did not stop within the configured timeout");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while stopping the heartbeat scheduler");
        }
        activeHeartbeats.clear();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void renew(RunExecutionHandle executionHandle, HeartbeatHandle heartbeat) {
        if (heartbeat.isStopped()) {
            return;
        }
        try {
            JobRunStore.LeaseRenewalResult result = runLeaseService.renewLease(
                    executionHandle.runId(), executionHandle.leaseToken(), leaseDuration);
            if (result != JobRunStore.LeaseRenewalResult.RENEWED) {
                LOG.warn("Heartbeat renewal lost ownership for run {} with result {}",
                        executionHandle.runId(), result);
                executionHandle.requestCancellation();
                heartbeat.stop();
            }
        } catch (RuntimeException exception) {
            LOG.warn("Heartbeat renewal failed for run {} with {}",
                    executionHandle.runId(), exception.getClass().getSimpleName());
            executionHandle.requestCancellation();
            heartbeat.stop();
        }
    }

    private static Duration positive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        if (value.toMillis() < 1) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
        return value;
    }

    private static final class HeartbeatThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "ReplicadbHeartbeat-" + sequence.incrementAndGet());
        }
    }
}