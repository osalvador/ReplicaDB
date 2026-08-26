package org.replicadb.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.replicadb.server.job.execution.AdmissionLane;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.replicadb.server.job.domain.JobRunStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

@Component
public final class ManagedRuntimeMetrics {

    public static final String CLAIMS = "replicadb.managed.claims";
    public static final String DISPATCHES = "replicadb.managed.dispatches";
    public static final String NOTIFICATIONS = "replicadb.managed.notifications";
    public static final String NOTIFICATION_CLAIM_LATENCY = "replicadb.managed.notification.claim.latency";
    public static final String POLLING_SCANS = "replicadb.managed.polling.scans";
    public static final String POLLING_LAG = "replicadb.managed.polling.lag";
    public static final String LEASE_RENEWALS = "replicadb.managed.lease.renewals";
    public static final String LEASE_RECOVERIES = "replicadb.managed.lease.recoveries";
    public static final String RETRIES = "replicadb.managed.retries";
    public static final String FENCED_UPDATES = "replicadb.managed.fenced.updates";
    public static final String CANCELLATIONS = "replicadb.managed.cancellations";
    public static final String TERMINAL_OUTCOMES = "replicadb.managed.terminal.outcomes";
    public static final String ACTIVE_WORKER_SLOTS = "replicadb.worker.active.slots";
    public static final String FREE_WORKER_SLOTS = "replicadb.worker.free.slots";
    public static final String ADMISSION_EVENTS = "replicadb.worker.admission.events";
    public static final String BUSY_SLOT_SECONDS = "replicadb.worker.busy.slot.seconds";
    public static final String NORMALIZED_BUSY_SLOT_SECONDS = "replicadb.worker.normalized.busy.slot.seconds";
    public static final String COMPLETED_RUNS = "replicadb.worker.completed.runs";
    public static final String LISTENER_CONNECTED = "replicadb.worker.listener.connected";
    public static final String POLLING_RUNNING = "replicadb.worker.polling.running";

    private static final Set<String> CLAIM_TYPES = Set.of("directed", "fallback", "queue", "other");
    private static final Set<String> CLAIM_OUTCOMES = Set.of("claimed", "empty", "error", "other");
    private static final Set<String> DISPATCH_TYPES = Set.of("manual", "scheduled", "retry", "recovery", "other");
    private static final Set<String> DISPATCH_OUTCOMES = Set.of("created", "replayed", "replacement", "noop", "error", "other");
    private static final Set<String> POLLING_TRIGGERS = Set.of("startup", "reconnect", "periodic", "manual", "other");
    private static final Set<String> POLLING_OUTCOMES = Set.of("success", "error", "skipped", "other");
    private static final Set<String> LEASE_OUTCOMES = Set.of("renewed", "fenced", "not_found", "error", "other");
    private static final Set<String> RETRY_TYPES = Set.of("manual", "automatic", "other");
    private static final Set<String> UPDATE_OPERATIONS = Set.of("progress", "succeeded", "failed", "cancelled", "other");
    private static final Set<String> UPDATE_OUTCOMES = Set.of("updated", "fenced", "not_found", "error", "other");
    private static final Set<String> CANCELLATION_OPERATIONS = Set.of(
            "request", "notification", "local", "pending", "completion", "other");
    private static final Set<String> CANCELLATION_OUTCOMES = Set.of(
            "requested", "already_requested", "cancelled", "terminal", "not_found",
            "published", "signalled", "missed", "updated", "fenced", "failed", "error", "other");
    private static final Set<String> RECOVERY_OUTCOMES = Set.of("replacement", "cancelled", "failed", "noop", "error", "other");
        private static final Set<String> ADMISSION_LANES = Set.of("directed", "fallback", "generic", "other");
        private static final Set<String> ADMISSION_OUTCOMES = Set.of(
            "claimed", "empty", "coalesced", "dropped", "error", "other");
        private static final Set<String> COMPLETED_RUN_OUTCOMES = Set.of(
            "succeeded", "failed", "cancelled", "retry_scheduled", "other");
        private static final Set<String> TERMINAL_STATUSES = Set.of(
            "pending", "running", "succeeded", "failed", "cancel_requested", "cancelled", "retry_scheduled");
    private static final ManagedRuntimeMetrics NOOP = new ManagedRuntimeMetrics(new SimpleMeterRegistry(), false);

    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeWorkerSlots = new AtomicInteger();
    private final AtomicInteger freeWorkerSlots = new AtomicInteger();
    private final AtomicBoolean listenerConnected = new AtomicBoolean();
    private final AtomicBoolean pollingRunning = new AtomicBoolean();
    private final boolean enabled;

    @Autowired
    public ManagedRuntimeMetrics(MeterRegistry meterRegistry) {
        this(meterRegistry, true);
    }

    private ManagedRuntimeMetrics(MeterRegistry meterRegistry, boolean enabled) {
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;
        if (enabled) {
            meterRegistry.gauge(ACTIVE_WORKER_SLOTS, activeWorkerSlots, AtomicInteger::get);
            meterRegistry.gauge(FREE_WORKER_SLOTS, freeWorkerSlots, AtomicInteger::get);
            meterRegistry.gauge(LISTENER_CONNECTED, listenerConnected, value -> value.get() ? 1 : 0);
            meterRegistry.gauge(POLLING_RUNNING, pollingRunning, value -> value.get() ? 1 : 0);
        }
    }

    public static ManagedRuntimeMetrics noop() {
        return NOOP;
    }

    public void recordClaim(String claimType, String outcome) {
        increment(CLAIMS, "claim_type", normalize(claimType, CLAIM_TYPES),
                "outcome", normalize(outcome, CLAIM_OUTCOMES));
    }

    public void recordAdmission(AdmissionLane lane, String outcome) {
        increment(ADMISSION_EVENTS,
                "lane", normalize(lane == null ? null : lane.name(), ADMISSION_LANES),
                "outcome", normalize(outcome, ADMISSION_OUTCOMES));
    }

    public void recordWorkerCompletedRun(String workerIdentity, String outcome) {
        increment(COMPLETED_RUNS,
                "worker_identity", WorkerMetricsIdentity.normalize(workerIdentity),
                "outcome", normalize(outcome, COMPLETED_RUN_OUTCOMES));
    }

    public WorkerBusySlotTracker createWorkerBusySlotTracker(String workerIdentity,
                                                              int maxConcurrentRuns,
                                                              LongSupplier nanoTimeSource) {
        return new WorkerBusySlotTracker(meterRegistry, WorkerMetricsIdentity.normalize(workerIdentity),
                maxConcurrentRuns, nanoTimeSource, enabled);
    }

    public void recordDispatch(String dispatchType, String outcome) {
        increment(DISPATCHES, "dispatch_type", normalize(dispatchType, DISPATCH_TYPES),
                "outcome", normalize(outcome, DISPATCH_OUTCOMES));
    }

    public void recordNotificationReceived(String channel, boolean accepted) {
        String normalizedChannel = RunNotificationPublisher.RUN_CHANNEL.equals(channel)
                ? "run"
                : RunNotificationPublisher.CONTROL_CHANNEL.equals(channel) ? "control" : "other";
        increment(NOTIFICATIONS, "channel", normalizedChannel,
                "outcome", accepted ? "accepted" : "rejected");
    }

    public void recordNotificationClaimLatencyNanos(long latencyNanos) {
        if (latencyNanos < 0) {
            latencyNanos = 0;
        }
        long measuredLatencyNanos = latencyNanos;
        safe(() -> meterRegistry.timer(NOTIFICATION_CLAIM_LATENCY)
                .record(measuredLatencyNanos, TimeUnit.NANOSECONDS));
    }

    public void recordPollingScan(String trigger, String outcome) {
        increment(POLLING_SCANS, "trigger", normalize(trigger, POLLING_TRIGGERS),
                "outcome", normalize(outcome, POLLING_OUTCOMES));
    }

    public void recordPollingLag(Instant oldestAvailableAt) {
        if (oldestAvailableAt == null) {
            return;
        }
        Duration lag = Duration.between(oldestAvailableAt, Instant.now());
        if (lag.isNegative()) {
            lag = Duration.ZERO;
        }
        Duration measuredLag = lag;
        safe(() -> meterRegistry.timer(POLLING_LAG).record(measuredLag));
    }

    public void recordLeaseRenewal(JobRunStore.LeaseRenewalResult result) {
        recordLeaseRenewal(result == null ? "error" : result.name());
    }

    public void recordLeaseRenewal(String outcome) {
        increment(LEASE_RENEWALS, "outcome", normalize(outcome, LEASE_OUTCOMES));
    }

    public void recordLeaseRecovery(String outcome) {
        increment(LEASE_RECOVERIES, "outcome", normalize(outcome, RECOVERY_OUTCOMES));
    }

    public void recordRetry(String retryType) {
        increment(RETRIES, "retry_type", normalize(retryType, RETRY_TYPES));
    }

    public void recordFencedUpdate(String operation, JobRunStore.FencedUpdateResult result) {
        recordFencedUpdate(operation, result == null ? "error" : result.name());
    }

    public void recordFencedUpdate(String operation, String outcome) {
        increment(FENCED_UPDATES, "operation", normalize(operation, UPDATE_OPERATIONS),
                "outcome", normalize(outcome, UPDATE_OUTCOMES));
    }

    public void recordCancellation(String operation, String outcome) {
        increment(CANCELLATIONS, "operation", normalize(operation, CANCELLATION_OPERATIONS),
                "outcome", normalize(outcome, CANCELLATION_OUTCOMES));
    }

    public void recordTerminalOutcome(JobRunStatus status) {
        String outcome = status == null ? "other" : normalize(status.name(), TERMINAL_STATUSES);
        increment(TERMINAL_OUTCOMES, "status", outcome);
    }

    public void updateWorkerCapacity(int activeSlots, int freeSlots) {
        set(activeWorkerSlots, activeSlots);
        set(freeWorkerSlots, freeSlots);
    }

    public void updateListenerConnected(boolean connected) {
        safe(() -> listenerConnected.set(connected));
    }

    public void updatePollingRunning(boolean running) {
        safe(() -> pollingRunning.set(running));
    }

    private void increment(String name, String... tags) {
        safe(() -> Counter.builder(name).tags(tags).register(meterRegistry).increment());
    }

    private void set(AtomicInteger value, int nextValue) {
        safe(() -> value.set(Math.max(0, nextValue)));
    }

    private void safe(Runnable operation) {
        if (!enabled) {
            return;
        }
        try {
            operation.run();
        } catch (RuntimeException ignored) {
        }
    }

    private static String normalize(String value, Set<String> allowed) {
        if (value == null) {
            return "other";
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        return allowed.contains(normalized) ? normalized : "other";
    }
}
