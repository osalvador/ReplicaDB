package org.replicadb.server.job.dispatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.application.RunDispatchResult;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class PollingFallback implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(PollingFallback.class);
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final WorkerDispatchCoordinator workerCoordinator;
    private final JobRunStore jobRunStore;
    private final RunDispatchService runDispatchService;
    private final String workerIdentity;
    private final Duration pollInterval;
    private final Duration shutdownTimeout;
    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final ManagedRuntimeMetrics metrics;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean scanInProgress = new AtomicBoolean();
    private volatile ScheduledFuture<?> periodicScan;

    public PollingFallback(WorkerDispatchCoordinator workerCoordinator,
                           JobRunStore jobRunStore,
                           RunDispatchService runDispatchService,
                           String workerIdentity,
                           Duration pollInterval,
                           int batchSize,
                           ScheduledExecutorService scheduler,
                           Duration shutdownTimeout) {
                this(workerCoordinator, jobRunStore, runDispatchService, workerIdentity, pollInterval,
                    batchSize, scheduler, shutdownTimeout, ManagedRuntimeMetrics.noop());
                }

                public PollingFallback(WorkerDispatchCoordinator workerCoordinator,
                           JobRunStore jobRunStore,
                           RunDispatchService runDispatchService,
                           String workerIdentity,
                           Duration pollInterval,
                           int batchSize,
                           ScheduledExecutorService scheduler,
                           Duration shutdownTimeout,
                           ManagedRuntimeMetrics metrics) {
        this.workerCoordinator = Objects.requireNonNull(workerCoordinator,
                "workerCoordinator must not be null");
        this.jobRunStore = Objects.requireNonNull(jobRunStore, "jobRunStore must not be null");
        this.runDispatchService = Objects.requireNonNull(runDispatchService,
                "runDispatchService must not be null");
        if (workerIdentity == null || workerIdentity.isBlank()) {
            throw new IllegalArgumentException("workerIdentity must not be blank");
        }
        this.workerIdentity = workerIdentity;
        this.pollInterval = positive(pollInterval, "pollInterval");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.shutdownTimeout = positive(shutdownTimeout, "shutdownTimeout");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    public PollingFallback(WorkerDispatchCoordinator workerCoordinator,
                           JobRunStore jobRunStore,
                           RunDispatchService runDispatchService,
                           String workerIdentity,
                           Duration pollInterval,
                           ScheduledExecutorService scheduler) {
        this(workerCoordinator, jobRunStore, runDispatchService, workerIdentity, pollInterval,
                DEFAULT_BATCH_SIZE, scheduler, DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public static ScheduledExecutorService newScheduler() {
        return java.util.concurrent.Executors.newSingleThreadScheduledExecutor(new PollingThreadFactory());
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        metrics.updatePollingRunning(true);
        scanIfAvailable("startup");
        periodicScan = scheduler.scheduleWithFixedDelay(() -> scanIfAvailable("periodic"),
                pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void onListenerReconnected() {
        if (!running.get()) {
            return;
        }
        try {
            scheduler.execute(() -> scanIfAvailable("reconnect"));
        } catch (RuntimeException exception) {
            LOG.debug("Polling reconnect scan could not be scheduled with {}",
                    exception.getClass().getSimpleName());
        }
    }

    public void scanNow() {
        scanIfAvailable("manual");
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        metrics.updatePollingRunning(false);
        ScheduledFuture<?> scan = periodicScan;
        if (scan != null) {
            scan.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
            LOG.warn("Interrupted while stopping worker polling");
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isScanInProgress() {
        return scanInProgress.get();
    }

    @Override
    public void close() {
        stop();
    }

    private void scanIfAvailable(String trigger) {
        if (!running.get()) {
            metrics.recordPollingScan(trigger, "skipped");
            return;
        }
        if (!scanInProgress.compareAndSet(false, true)) {
            metrics.recordPollingScan(trigger, "skipped");
            return;
        }
        try {
            workerCoordinator.signalEligibleWork();
            signalCancellationRequests();
            recoverExpiredRuns();
            recordPollingLag();
            metrics.recordPollingScan(trigger, "success");
        } catch (RuntimeException exception) {
            metrics.recordPollingScan(trigger, "error");
            LOG.warn("Worker polling scan failed with {}", exception.getClass().getSimpleName());
        } finally {
            scanInProgress.set(false);
        }
    }

    private void recordPollingLag() {
        try {
            JobRunStore.EligibleRunSnapshot snapshot = jobRunStore.findEligibleRunSnapshot(1);
            if (snapshot != null) {
                metrics.recordPollingLag(snapshot.oldestAvailableAt());
            }
        } catch (RuntimeException ignored) {
        }
    }

    private void signalCancellationRequests() {
        List<UUID> runIds = jobRunStore.findCancellationRequestedRunIds(workerIdentity, batchSize);
        for (UUID runId : runIds) {
            workerCoordinator.signalCancellation(runId);
        }
    }

    private void recoverExpiredRuns() {
        List<UUID> runIds = jobRunStore.findExpiredRunIds(batchSize);
        for (UUID runId : runIds) {
            RunDispatchResult result = runDispatchService.recoverExpiredRun(runId);
            if (result.replacementCreated()) {
                result.run().ifPresent(replacement -> workerCoordinator.signalRun(replacement.id()));
            }
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

    private static final class PollingThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "ReplicadbWorkerPolling-" + sequence.incrementAndGet());
        }
    }
}
