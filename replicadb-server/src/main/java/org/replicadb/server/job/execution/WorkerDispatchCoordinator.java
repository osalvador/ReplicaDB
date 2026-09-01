package org.replicadb.server.job.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.config.WorkerRuntimeProperties;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.observability.WorkerBusySlotTracker;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
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
    private final ManagedRuntimeMetrics metrics;
    private final WorkerAdmissionPolicy admissionPolicy;
    private final WorkerAdmissionQueue admissionQueue;
    private final WorkerAdmissionScheduler admissionScheduler;
    private final WorkerBusySlotTracker busySlotTracker;
    private final ExecutorService executor;
    private final Semaphore capacity;
    private final int maxConcurrentRuns;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicInteger scheduledAdmissions = new AtomicInteger();
    private final Object admissionLock = new Object();

    @Deprecated
    public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                     JobRunStore jobRunStore,
                                     JobExecutionService jobExecutionService,
                                     ActiveRunRegistry activeRunRegistry,
                                     HeartbeatService heartbeatService,
                                     WorkerRunIdentity workerIdentity,
                                     int maxConcurrentRuns,
                                     Duration leaseDuration,
                                     Duration shutdownTimeout) {
                        this(runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
                            workerIdentity, maxConcurrentRuns, leaseDuration, shutdownTimeout,
                            ManagedRuntimeMetrics.noop());
                        }

                        @Deprecated
                        public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                         JobRunStore jobRunStore,
                                         JobExecutionService jobExecutionService,
                                         ActiveRunRegistry activeRunRegistry,
                                         HeartbeatService heartbeatService,
                                         WorkerRunIdentity workerIdentity,
                                         int maxConcurrentRuns,
                                         Duration leaseDuration,
                                         Duration shutdownTimeout,
                                         ManagedRuntimeMetrics metrics) {
                            this(runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
                                workerIdentity, maxConcurrentRuns, leaseDuration, shutdownTimeout, metrics,
                                new WorkerAdmissionPolicy(new WorkerRuntimeProperties.Admission()),
                                new WorkerAdmissionScheduler(),
                                metrics.createWorkerBusySlotTracker(workerIdentity.value(), maxConcurrentRuns, System::nanoTime));
                            }

                            public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                             JobRunStore jobRunStore,
                                             JobExecutionService jobExecutionService,
                                             ActiveRunRegistry activeRunRegistry,
                                             HeartbeatService heartbeatService,
                                             WorkerRunIdentity workerIdentity,
                                             int maxConcurrentRuns,
                                             Duration leaseDuration,
                                             Duration shutdownTimeout,
                                             ManagedRuntimeMetrics metrics,
                                             WorkerAdmissionPolicy admissionPolicy,
                                             WorkerAdmissionScheduler admissionScheduler,
                                             WorkerBusySlotTracker busySlotTracker) {
                            this(runLeaseService, jobRunStore, jobExecutionService, activeRunRegistry, heartbeatService,
                                workerIdentity, maxConcurrentRuns, leaseDuration, shutdownTimeout, metrics,
                                admissionPolicy, admissionScheduler, busySlotTracker, 1_024);
                            }

                            public WorkerDispatchCoordinator(RunLeaseService runLeaseService,
                                             JobRunStore jobRunStore,
                                             JobExecutionService jobExecutionService,
                                             ActiveRunRegistry activeRunRegistry,
                                             HeartbeatService heartbeatService,
                                             WorkerRunIdentity workerIdentity,
                                             int maxConcurrentRuns,
                                             Duration leaseDuration,
                                             Duration shutdownTimeout,
                                             ManagedRuntimeMetrics metrics,
                                             WorkerAdmissionPolicy admissionPolicy,
                                             WorkerAdmissionScheduler admissionScheduler,
                                             WorkerBusySlotTracker busySlotTracker,
                                             int directedQueueCapacity) {
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
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
        this.admissionScheduler = Objects.requireNonNull(admissionScheduler, "admissionScheduler must not be null");
        this.busySlotTracker = Objects.requireNonNull(busySlotTracker, "busySlotTracker must not be null");
        if (directedQueueCapacity < 1) {
            throw new IllegalArgumentException("directedQueueCapacity must be positive");
        }
        this.maxConcurrentRuns = maxConcurrentRuns;
        this.capacity = new Semaphore(maxConcurrentRuns);
        this.admissionQueue = new WorkerAdmissionQueue(directedQueueCapacity);
        this.executor = new ThreadPoolExecutor(
                maxConcurrentRuns,
                maxConcurrentRuns,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new WorkerThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
            this.metrics.updateWorkerCapacity(0, maxConcurrentRuns);
    }

    @Deprecated
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

    @Deprecated
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
        signalRun(runId, null);
    }

    public void signalRun(UUID runId, long notificationReceivedNanos) {
        Objects.requireNonNull(runId, "runId must not be null");
        signalRun(runId, Long.valueOf(notificationReceivedNanos));
    }

    private void signalRun(UUID runId, Long notificationReceivedNanos) {
        if (!accepting.get()) {
            return;
        }
        WorkerAdmissionQueue.OfferResult result = admissionQueue.offerDirected(
                runId, notificationReceivedNanos == null ? -1 : notificationReceivedNanos);
        switch (result) {
            case ADDED -> schedulePendingAdmissions();
            case COALESCED -> {
                admissionPolicy.recordDuplicateSignal();
                metrics.recordAdmission(AdmissionLane.DIRECTED, "coalesced");
            }
            case DROPPED -> metrics.recordAdmission(AdmissionLane.DIRECTED, "dropped");
        }
    }

    public void signalEligibleWork() {
        requestGenericRefill("manual");
    }

    public void requestGenericRefill(String trigger) {
        if (!accepting.get()) {
            return;
        }
        if (!admissionQueue.requestGenericRefill(trigger)) {
            admissionPolicy.recordDuplicateSignal();
            metrics.recordAdmission(AdmissionLane.GENERIC, "coalesced");
        }
        schedulePendingAdmissions();
    }

    public boolean signalCancellation(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        boolean signalled = activeRunRegistry.requestCancellation(runId);
        metrics.recordCancellation("local", signalled ? "signalled" : "missed");
        return signalled;
    }

    public void startAccepting() {
        if (executor.isShutdown() || admissionScheduler.isShutdown()) {
            throw new IllegalStateException("Worker executor is shut down");
        }
        accepting.set(true);
        updateCapacityMetrics();
    }

    public void stopAccepting() {
        accepting.set(false);
        updateCapacityMetrics();
    }

    public void shutdown() {
        shutdown(shutdownTimeout);
    }

    public void shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        stopAccepting();
        cancelPendingAdmissionsAndActiveRuns(timeout);
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
        updateCapacityMetrics();
    }

    public void cancelPendingAdmissionsAndActiveRuns(Duration timeout) {
        admissionQueue.clear();
        admissionScheduler.shutdown(positive(timeout, "timeout"));
        activeRunRegistry.requestCancellationForAll();
    }

    public boolean isShutdown() {
        return executor.isShutdown() && admissionScheduler.isShutdown();
    }

    public boolean isAccepting() {
        return accepting.get() && !executor.isShutdown() && !admissionScheduler.isShutdown();
    }

    public int maxConcurrentRuns() {
        return maxConcurrentRuns;
    }

    public int availableCapacity() {
        return capacity.availablePermits();
    }

    @Override
    public void close() {
        shutdown();
    }

    private boolean claimAndExecute(UUID runId, Long notificationReceivedNanos) {
        if (!accepting.get()) {
            return false;
        }
        Optional<ClaimedRunPreparation> claimed = runLeaseService.claimAndPrepare(
            runId, workerIdentity.value(), leaseDuration);
        metrics.recordAdmission(AdmissionLane.DIRECTED, claimed.isPresent() ? "claimed" : "empty");
        if (claimed.isEmpty()) {
            claimed = runLeaseService.claimAndPrepare(null, workerIdentity.value(), leaseDuration);
            metrics.recordAdmission(AdmissionLane.FALLBACK, claimed.isPresent() ? "claimed" : "empty");
        }
        claimed.ifPresent(preparation -> {
            if (notificationReceivedNanos != null) {
                metrics.recordNotificationClaimLatencyNanos(
                        System.nanoTime() - notificationReceivedNanos);
            }
            admissionPolicy.recordSuccessfulClaim();
            executeClaimedRun(preparation);
        });
        if (claimed.isEmpty()) {
            admissionPolicy.recordContention();
        }
        return claimed.isPresent();
    }

    private boolean claimAndExecuteNext() {
        if (!accepting.get()) {
            return false;
        }
        Optional<ClaimedRunPreparation> claimed = runLeaseService.claimAndPrepare(
            null, workerIdentity.value(), leaseDuration);
        metrics.recordAdmission(AdmissionLane.GENERIC, claimed.isPresent() ? "claimed" : "empty");
        if (claimed.isPresent()) {
            admissionPolicy.recordSuccessfulClaim();
        } else {
            admissionPolicy.recordContention();
        }
        claimed.ifPresent(this::executeClaimedRun);
        return claimed.isPresent();
    }

    private JobRunOutcome executeClaimedRun(ClaimedRunPreparation preparation) {
        JobRun run = preparation.run();
        AtomicReference<RunExecutionHandle> executionHandle = new AtomicReference<>();
        AtomicReference<HeartbeatHandle> heartbeat = new AtomicReference<>();
        try {
            JobRunOutcome outcome = jobExecutionService.executeClaimedRun(preparation, handle -> {
                executionHandle.set(handle);
                if (isCancellationRequested(handle.runId())) {
                    handle.requestCancellation();
                }
                heartbeat.set(heartbeatService.start(handle));
            });
            if (outcome != null) {
                metrics.recordWorkerCompletedRun(workerIdentity.value(), outcome.status().name());
            }
            return outcome;
        } catch (RuntimeException exception) {
            LOG.warn("Worker execution coordination failed for run {} with {}",
                    run.id(), exception.getClass().getSimpleName());
            return null;
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

    private void schedulePendingAdmissions() {
        synchronized (admissionLock) {
            if (!accepting.get()) {
                return;
            }
            while (capacity.availablePermits() - scheduledAdmissions.get() > 0) {
                Optional<WorkerAdmissionQueue.DirectedSignal> directed = admissionQueue.pollDirected();
                if (directed.isPresent()) {
                    WorkerAdmissionQueue.DirectedSignal signal = directed.orElseThrow();
                    scheduleAdmission(AdmissionLane.DIRECTED, admissionPolicy.delayFor(AdmissionLane.DIRECTED),
                            () -> dispatchDirected(signal), signal.runId(), null);
                    continue;
                }

                Optional<String> genericTrigger = admissionQueue.pollGenericRefill();
                if (genericTrigger.isEmpty()) {
                    return;
                }
                int availableSlots = capacity.availablePermits() - scheduledAdmissions.get();
                if (availableSlots < 1) {
                    admissionQueue.restoreGenericRefill(genericTrigger.orElseThrow());
                    return;
                }
                for (int index = 0; index < availableSlots; index++) {
                    String trigger = genericTrigger.orElseThrow();
                    scheduleAdmission(AdmissionLane.GENERIC, admissionPolicy.delayFor(AdmissionLane.GENERIC),
                            () -> dispatchGeneric(trigger), null, trigger);
                }
            }
        }
    }

    private void scheduleAdmission(AdmissionLane lane, Duration delay, Runnable action,
                                   UUID directedRunId, String genericTrigger) {
        scheduledAdmissions.incrementAndGet();
        try {
            admissionScheduler.schedule(() -> {
                scheduledAdmissions.decrementAndGet();
                action.run();
            }, delay);
        } catch (RuntimeException exception) {
            scheduledAdmissions.decrementAndGet();
            if (directedRunId != null) {
                admissionQueue.requeueDirected(directedRunId);
            } else {
                admissionQueue.restoreGenericRefill(genericTrigger);
            }
            metrics.recordAdmission(lane, "error");
            if (accepting.get()) {
                LOG.warn("Worker admission scheduling failed with {}",
                        exception.getClass().getSimpleName());
            }
        }
    }

    private void dispatchDirected(WorkerAdmissionQueue.DirectedSignal signal) {
        if (!accepting.get()) {
            admissionQueue.completeDirected(signal.runId());
            return;
        }
        if (!capacity.tryAcquire()) {
            admissionQueue.requeueDirected(signal.runId());
            schedulePendingAdmissions();
            return;
        }
            submitClaim(() -> claimAndExecute(signal.runId(),
                        signal.receivedNanos() < 0 ? null : signal.receivedNanos()),
                signal.runId(), null);
    }

    private void dispatchGeneric(String trigger) {
        if (!accepting.get()) {
            return;
        }
        if (!capacity.tryAcquire()) {
            admissionQueue.restoreGenericRefill(trigger);
            return;
        }
        submitClaim(this::claimAndExecuteNext, null, trigger);
    }

    private void submitClaim(ClaimAction claimAction, UUID directedRunId, String genericTrigger) {
        busySlotTracker.slotAcquired();
        updateCapacityMetrics();
        try {
            executor.execute(() -> {
                boolean claimed = false;
                try {
                    claimed = claimAction.run();
                } catch (RuntimeException exception) {
                    LOG.warn("Worker claim coordination failed with {}",
                            exception.getClass().getSimpleName());
                    metrics.recordAdmission(directedRunId == null ? AdmissionLane.GENERIC : AdmissionLane.DIRECTED,
                            "error");
                } finally {
                    if (directedRunId != null) {
                        admissionQueue.completeDirected(directedRunId);
                    }
                    busySlotTracker.slotReleased();
                    capacity.release();
                    updateCapacityMetrics();
                    if (claimed) {
                        requestGenericRefill("completion");
                    }
                    schedulePendingAdmissions();
                }
            });
        } catch (RejectedExecutionException exception) {
            if (directedRunId != null) {
                admissionQueue.completeDirected(directedRunId);
            } else {
                admissionQueue.restoreGenericRefill(genericTrigger);
            }
            busySlotTracker.slotReleased();
            capacity.release();
            updateCapacityMetrics();
            if (accepting.get()) {
                LOG.debug("Worker admission was rejected while shutting down");
            }
        }
    }

    @FunctionalInterface
    private interface ClaimAction {
        boolean run();
    }

    private void updateCapacityMetrics() {
        int available = capacity.availablePermits();
        metrics.updateWorkerCapacity(maxConcurrentRuns - available, available);
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
