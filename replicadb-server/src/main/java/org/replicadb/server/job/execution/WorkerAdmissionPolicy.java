package org.replicadb.server.job.execution;

import org.replicadb.server.job.config.WorkerRuntimeProperties;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

public final class WorkerAdmissionPolicy {

    private final Duration jitterMax;
    private final Duration genericCooldown;
    private final long jitterMaxNanos;
    private final long genericCooldownNanos;
    private final ContentionBackoff contentionBackoff;
    private final LongSupplier nanoTimeSource;
    private final DoubleSupplier randomSource;
    private long genericCooldownUntilNanos;

    public WorkerAdmissionPolicy(WorkerRuntimeProperties.Admission configuration) {
        this(configuration, System::nanoTime, () -> ThreadLocalRandom.current().nextDouble());
    }

    public WorkerAdmissionPolicy(WorkerRuntimeProperties.Admission configuration,
                                 LongSupplier nanoTimeSource, DoubleSupplier randomSource) {
        this(Objects.requireNonNull(configuration, "configuration must not be null").getJitterMax(),
                configuration.getGenericCooldown(),
                configuration.getAdaptiveBackoff(), nanoTimeSource, randomSource);
    }

    public WorkerAdmissionPolicy(Duration jitterMax, Duration genericCooldown,
                                 WorkerRuntimeProperties.AdaptiveBackoff backoffConfiguration,
                                 LongSupplier nanoTimeSource, DoubleSupplier randomSource) {
        this.jitterMax = nonNegative(jitterMax, "jitterMax");
        this.genericCooldown = nonNegative(genericCooldown, "genericCooldown");
        this.jitterMaxNanos = toNanos(this.jitterMax, "jitterMax");
        this.genericCooldownNanos = toNanos(this.genericCooldown, "genericCooldown");
        this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource must not be null");
        this.randomSource = Objects.requireNonNull(randomSource, "randomSource must not be null");
        this.contentionBackoff = new ContentionBackoff(backoffConfiguration, nanoTimeSource);
        this.genericCooldownUntilNanos = nanoTimeSource.getAsLong();
    }

    public synchronized Duration delayFor(AdmissionLane lane) {
        Objects.requireNonNull(lane, "lane must not be null");
        return switch (lane) {
            case DIRECTED -> jitter();
            case FALLBACK -> Duration.ZERO;
            case GENERIC -> add(jitter(), genericDelay());
        };
    }

    public synchronized void recordSuccessfulClaim() {
        long now = nanoTimeSource.getAsLong();
        genericCooldownUntilNanos = saturatingAdd(now, genericCooldownNanos);
        contentionBackoff.reset();
    }

    public void recordContention() {
        contentionBackoff.recordContention();
    }

    public void recordDuplicateSignal() {
        contentionBackoff.recordContention();
    }

    public void recordUncontendedWork() {
        contentionBackoff.recordUncontendedWork();
    }

    public synchronized void reset() {
        genericCooldownUntilNanos = nanoTimeSource.getAsLong();
        contentionBackoff.reset();
    }

    public Duration contentionDelay() {
        return contentionBackoff.currentDelay();
    }

    public boolean isBackoffEnabled() {
        return contentionBackoff.isEnabled();
    }

    private Duration genericDelay() {
        long now = nanoTimeSource.getAsLong();
        long cooldownRemaining = Math.max(0, genericCooldownUntilNanos - now);
        return max(Duration.ofNanos(cooldownRemaining), contentionBackoff.currentDelay());
    }

    private Duration jitter() {
        if (jitterMaxNanos == 0) {
            return Duration.ZERO;
        }
        double random = randomSource.getAsDouble();
        if (Double.isNaN(random) || random <= 0) {
            return Duration.ZERO;
        }
        if (random >= 1) {
            return jitterMax;
        }
        return Duration.ofNanos(Math.round(jitterMaxNanos * random));
    }

    private static Duration nonNegative(Duration duration, String name) {
        if (duration == null || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return duration;
    }

    private static long toNanos(Duration duration, String name) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(name + " is too large", exception);
        }
    }

    private static Duration max(Duration first, Duration second) {
        return first.compareTo(second) >= 0 ? first : second;
    }

    private static Duration add(Duration first, Duration second) {
        return Duration.ofNanos(saturatingAdd(first.toNanos(), second.toNanos()));
    }

    private static long saturatingAdd(long first, long second) {
        if (second > 0 && first > Long.MAX_VALUE - second) {
            return Long.MAX_VALUE;
        }
        if (second < 0 && first < Long.MIN_VALUE - second) {
            return Long.MIN_VALUE;
        }
        return first + second;
    }
}