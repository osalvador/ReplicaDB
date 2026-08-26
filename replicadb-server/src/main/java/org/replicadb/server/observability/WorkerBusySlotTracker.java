package org.replicadb.server.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class WorkerBusySlotTracker {

    private final String workerIdentity;
    private final int maxConcurrentRuns;
    private final LongSupplier nanoTimeSource;
    private int activeSlots;
    private long lastUpdatedNanos;
    private double busySlotSeconds;
    private double normalizedBusySlotSeconds;

    public WorkerBusySlotTracker(MeterRegistry meterRegistry, String workerIdentity,
                                 int maxConcurrentRuns, LongSupplier nanoTimeSource) {
        this(meterRegistry, workerIdentity, maxConcurrentRuns, nanoTimeSource, true);
    }

    WorkerBusySlotTracker(MeterRegistry meterRegistry, String workerIdentity,
                          int maxConcurrentRuns, LongSupplier nanoTimeSource, boolean metricsEnabled) {
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.workerIdentity = WorkerMetricsIdentity.normalize(workerIdentity);
        if (maxConcurrentRuns < 1) {
            throw new IllegalArgumentException("maxConcurrentRuns must be positive");
        }
        this.maxConcurrentRuns = maxConcurrentRuns;
        this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource must not be null");
        this.lastUpdatedNanos = nanoTimeSource.getAsLong();
        if (metricsEnabled) {
            registerMeters(meterRegistry);
        }
    }

    public synchronized void slotAcquired() {
        updateElapsed();
        if (activeSlots < maxConcurrentRuns) {
            activeSlots++;
        }
    }

    public synchronized void slotReleased() {
        updateElapsed();
        if (activeSlots > 0) {
            activeSlots--;
        }
    }

    public synchronized Snapshot snapshot() {
        updateElapsed();
        return new Snapshot(workerIdentity, maxConcurrentRuns, activeSlots,
                busySlotSeconds, normalizedBusySlotSeconds);
    }

    public synchronized double busySlotSeconds() {
        updateElapsed();
        return busySlotSeconds;
    }

    public synchronized double normalizedBusySlotSeconds() {
        updateElapsed();
        return normalizedBusySlotSeconds;
    }

    public synchronized int activeSlots() {
        return activeSlots;
    }

    private void registerMeters(MeterRegistry meterRegistry) {
        Tags tags = Tags.of("worker_identity", workerIdentity);
        try {
            meterRegistry.gauge(BUSY_SLOT_SECONDS, tags, this, WorkerBusySlotTracker::busySlotSeconds);
            meterRegistry.gauge(NORMALIZED_BUSY_SLOT_SECONDS, tags, this,
                    WorkerBusySlotTracker::normalizedBusySlotSeconds);
        } catch (RuntimeException ignored) {
        }
    }

    private void updateElapsed() {
        long now = nanoTimeSource.getAsLong();
        long elapsedNanos = now - lastUpdatedNanos;
        if (elapsedNanos > 0 && activeSlots > 0) {
            double elapsedSeconds = elapsedNanos / (double) Duration.ofSeconds(1).toNanos();
            busySlotSeconds += activeSlots * elapsedSeconds;
            normalizedBusySlotSeconds += (activeSlots / (double) maxConcurrentRuns) * elapsedSeconds;
        }
        if (elapsedNanos >= 0) {
            lastUpdatedNanos = now;
        }
    }

    private static final String BUSY_SLOT_SECONDS = ManagedRuntimeMetrics.BUSY_SLOT_SECONDS;
    private static final String NORMALIZED_BUSY_SLOT_SECONDS = ManagedRuntimeMetrics.NORMALIZED_BUSY_SLOT_SECONDS;

    public record Snapshot(String workerIdentity, int maxConcurrentRuns, int activeSlots,
                           double busySlotSeconds, double normalizedBusySlotSeconds) {
    }
}