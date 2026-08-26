package org.replicadb.server.job.execution;

import org.replicadb.server.job.config.WorkerRuntimeProperties;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class ContentionBackoff {

    private final boolean enabled;
    private final long initialDelayNanos;
    private final long maxDelayNanos;
    private final long decayHalfLifeNanos;
    private final LongSupplier nanoTimeSource;
    private double contentionScore;
    private long lastUpdatedNanos;

    public ContentionBackoff(WorkerRuntimeProperties.AdaptiveBackoff configuration,
                             LongSupplier nanoTimeSource) {
        this(Objects.requireNonNull(configuration, "configuration must not be null").isEnabled(),
                configuration.getInitialDelay(), configuration.getMaxDelay(),
                configuration.getDecayHalfLife(), nanoTimeSource);
    }

    public ContentionBackoff(boolean enabled, Duration initialDelay, Duration maxDelay,
                             Duration decayHalfLife, LongSupplier nanoTimeSource) {
        this.enabled = enabled;
        this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource must not be null");
        if (!enabled) {
            this.initialDelayNanos = 0;
            this.maxDelayNanos = 0;
            this.decayHalfLifeNanos = 0;
            this.lastUpdatedNanos = nanoTimeSource.getAsLong();
            return;
        }
        this.initialDelayNanos = positiveNanos(initialDelay, "initialDelay");
        this.maxDelayNanos = positiveNanos(maxDelay, "maxDelay");
        this.decayHalfLifeNanos = positiveNanos(decayHalfLife, "decayHalfLife");
        if (maxDelayNanos < initialDelayNanos) {
            throw new IllegalArgumentException("maxDelay must not be less than initialDelay");
        }
        this.lastUpdatedNanos = nanoTimeSource.getAsLong();
    }

    public synchronized Duration currentDelay() {
        refresh();
        if (!enabled || contentionScore <= 0) {
            return Duration.ZERO;
        }
        double delayNanos = initialDelayNanos * Math.pow(2, contentionScore - 1);
        if (delayNanos >= maxDelayNanos) {
            return Duration.ofNanos(maxDelayNanos);
        }
        return Duration.ofNanos(Math.max(1, Math.round(delayNanos)));
    }

    public synchronized void recordContention() {
        refresh();
        contentionScore = Math.min(contentionScore + 1, 64);
    }

    public synchronized void recordUncontendedWork() {
        refresh();
        contentionScore *= 0.5;
    }

    public synchronized void reset() {
        contentionScore = 0;
        lastUpdatedNanos = nanoTimeSource.getAsLong();
    }

    public boolean isEnabled() {
        return enabled;
    }

    private void refresh() {
        if (!enabled) {
            return;
        }
        long now = nanoTimeSource.getAsLong();
        long elapsed = now - lastUpdatedNanos;
        if (elapsed > 0 && contentionScore > 0) {
            contentionScore *= Math.pow(0.5, (double) elapsed / decayHalfLifeNanos);
        }
        lastUpdatedNanos = now;
    }

    private static long positiveNanos(Duration duration, String name) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(name + " is too large", exception);
        }
    }
}