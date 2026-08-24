package org.replicadb.server.job.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class WorkerDispatchCoordinator implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(WorkerDispatchCoordinator.class);
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(5);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final RunLeaseService runLeaseService;
    private final JobRunStore jobRunStore;
    private final JobExecutionService jobExecutionService;
    private final ActiveRunRegistry activeRunRegistry;
    private final HeartbeatService heartbeatService;
    private final WorkerRunIdentity workerIdentity;
    private final Duration leaseDuration;
    private final Duration shutdownTimeout;
    private final ExecutorService executor;
    private final Semaphore capacity;
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                     JobRunStore jobRunStore,
                                     JobExecutionService jobExecutionService,
                                     ActiveRunRegistry activeRunRegistry,
                                     HeartbeatService heartbeatService,
                                     WorkerRunIdentity workerIdentity,
                                     int maxConcurrentRuns,
                                     Duration leaseDuration,
                                     Duration shutdownTimeout) {
        this.runLeaseService = Objects.requireNonNull(runLeaseService, "runLeaseService must not be null");
        this.jobRunStore = Objects.requireNonNull(jobRunStore, "jobRunStore must not be null");
        this.jobExecutionService = Objects.requireNonNull(jobExecutionService,
                "jobExecutionService must not be null");
        this.activeRunRegistry = Objects.requireNonNull(activeRunRegistry,
                "activeRunRegistry must not be null");
        this.heartbeatService = Objects.requireNonNull(heartbeatService, "heartbeatService must not be null");
        this.workerIdentity = Objects.requireNonNull(workerIdentity, "workerIdentity must not be null");
        if (maxConcurrentRuns < 1) {
            throw new IllegalArgumentException("maxConcurrentRuns must be positive");
        }
        this.leaseDuration = positive(leaseDuration, "leaseDuration");
        this.shutdownTimeout = positive(shutdownTimeout, "shutdownTimeout");
        this.capacity = new Semaphore(maxConcurrentRuns);
        this.executor = new ThreadPoolExecutor(
                maxConcurrentRuns,
                maxConcurrentRuns,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new WorkerThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                     JobRunStore jobRunStore,
                                     JobExecutionService jobExecutionService,
                                     ActiveRunRegistry activeRunRegistry,
                                     HeartbeatService heartbeatService,
                                     WorkerRunIdentity workerIdentity,
                                     int maxConcurrentRuns,
                                     Duration leaseDuration) {
        this(runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
                workerIdentity, maxConcurrentRuns, leaseDuration, DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                     JobRunStore jobRunStore,
                                     JobExecutionService jobExecutionService,
                                     ActiveRunRegistry activeRunRegistry,
                                     HeartbeatService heartbeatService,
                                     String configuredIdentity,
                                     int maxConcurrentRuns,
                                     Duration leaseDuration) {
        this(runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
                WorkerRunIdentity.resolve(configuredIdentity), maxConcurrentRuns, leaseDuration);
    }

    public WorkerRunIdentity workerIdentity() {
        return workerIdentity;
    }

    public void signalRun(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        dispatch(() -> claimAndExecute(runId));
    }

    public void signalEligibleWork() {
        dispatch(this::claimAndExecuteNext);
    }

    public boolean signalCancellation(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        return activeRunRegistry.requestCancellation(runId);
    }

    public void startAccepting() {
        if (executor.isShutdown()) {
            throw new IllegalStateException("Worker executor is shut down");
        }
        accepting.set(true);
    }

    public void stopAccepting() {
        accepting.set(false);
    }

    public void shutdown() {
        shutdown(shutdownTimeout);
    }

    public void shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        stopAccepting();
        activeRunRegistry.requestCancellationForAll();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(positive(timeout, "timeout").toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(positive(timeout, "timeout").toMillis(), TimeUnit.MILLISECONDS)) {
                    LOG.warn("Worker executor did not stop within the configured timeout");
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
            LOG.warn("Interrupted while stopping the worker executor");
        }
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void claimAndExecute(UUID runId) {
        if (!accepting.get()) {
            return;
        }
        Optional<JobRun> claimed = runLeaseService.claimRequested(runId, workerIdentity.value(), leaseDuration);
        claimed.ifPresent(this::executeClaimedRun);
    }

    private void claimAndExecuteNext() {
        if (!accepting.get()) {
            return;
        }
        Optional<JobRun> claimed = runLeaseService.claimNextEligible(workerIdentity.value(), leaseDuration);
        claimed.ifPresent(this::executeClaimedRun);
    }

    private void executeClaimedRun(JobRun run) {
        AtomicReference<RunExecutionHandle> executionHandle = new AtomicReference<>();
        AtomicReference<HeartbeatHandle> heartbeat = new AtomicReference<>();
        try {
            jobExecutionService.executeClaimedRun(run, handle -> {
                executionHandle.set(handle);
                if (isCancellationRequested(handle.runId())) {
                    handle.requestCancellation();
                }
                heartbeat.set(heartbeatService.start(handle));
            });
        } catch (RuntimeException exception) {
            LOG.warn("Worker execution coordination failed for run {} with {}",
                    run.id(), exception.getClass().getSimpleName());
        } finally {
            HeartbeatHandle heartbeatHandle = heartbeat.get();
            if (heartbeatHandle != null) {
                heartbeatHandle.stop();
            }
            RunExecutionHandle handle = executionHandle.get();
            if (handle != null) {
                activeRunRegistry.remove(run.id(), handle);
            } else {
                activeRunRegistry.remove(run.id());
            }
        }
    }

    private boolean isCancellationRequested(UUID runId) {
        return jobRunStore.findById(runId)
                .map(JobRun::status)
                .filter(status -> status == JobRunStatus.CANCEL_REQUESTED)
                .isPresent();
    }

    private void dispatch(Runnable claimAction) {
        if (!accepting.get() || !capacity.tryAcquire()) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    claimAction.run();
                } catch (RuntimeException exception) {
                    LOG.warn("Worker claim coordination failed with {}",
                            exception.getClass().getSimpleName());
                } finally {
                    capacity.release();
                }
            });
        } catch (RejectedExecutionException exception) {
            capacity.release();
            if (accepting.get()) {
                LOG.debug("Worker wake-up was rejected while at capacity");
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

    private static final class WorkerThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "ReplicadbWorkerRun-" + sequence.incrementAndGet());
        }
    }
}