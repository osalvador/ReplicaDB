package org.replicadb.server.job.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "replicadb.worker")
public class WorkerRuntimeProperties {

    private static final int MAX_DIRECTED_QUEUE_CAPACITY = 100_000;

    private String identity = "";
    private int maxConcurrentRuns = 1;
    private Duration leaseDuration = Duration.ofMinutes(5);
    private Duration heartbeatInterval = Duration.ofSeconds(30);
    private Duration pollInterval = Duration.ofSeconds(30);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private int pollBatchSize = 100;
    private Listener listener = new Listener();
    private Admission admission = new Admission();

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public int getMaxConcurrentRuns() {
        return maxConcurrentRuns;
    }

    public void setMaxConcurrentRuns(int maxConcurrentRuns) {
        this.maxConcurrentRuns = maxConcurrentRuns;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(Duration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public int getPollBatchSize() {
        return pollBatchSize;
    }

    public void setPollBatchSize(int pollBatchSize) {
        this.pollBatchSize = pollBatchSize;
    }

    public Listener getListener() {
        return listener;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }

    public Admission getAdmission() {
        return admission;
    }

    public void setAdmission(Admission admission) {
        this.admission = admission;
    }

    public void validate(int datasourcePoolSize) {
        if (maxConcurrentRuns < 1) {
            throw new IllegalArgumentException("replicadb.worker.max-concurrent-runs must be positive");
        }
        positive(leaseDuration, "replicadb.worker.lease-duration");
        positive(heartbeatInterval, "replicadb.worker.heartbeat-interval");
        positive(pollInterval, "replicadb.worker.poll-interval");
        positive(shutdownTimeout, "replicadb.worker.shutdown-timeout");
        if (pollBatchSize < 1) {
            throw new IllegalArgumentException("replicadb.worker.poll-batch-size must be positive");
        }
        if (listener == null) {
            throw new IllegalArgumentException("replicadb.worker.listener must not be null");
        }
        positive(listener.initialReconnectDelay, "replicadb.worker.listener.initial-reconnect-delay");
        positive(listener.maxReconnectDelay, "replicadb.worker.listener.max-reconnect-delay");
        if (listener.maxReconnectDelay.compareTo(listener.initialReconnectDelay) < 0) {
            throw new IllegalArgumentException(
                    "replicadb.worker.listener.max-reconnect-delay must not be less than initial-reconnect-delay");
        }
        validateAdmission();
        if (datasourcePoolSize < maxConcurrentRuns + 4) {
            throw new IllegalArgumentException(
                    "spring.datasource.hikari.maximum-pool-size must be at least "
                            + (maxConcurrentRuns + 4) + " for the configured worker concurrency");
        }
    }

    private void validateAdmission() {
        if (admission == null) {
            throw new IllegalArgumentException("replicadb.worker.admission must not be null");
        }
        nonNegative(admission.jitterMax, "replicadb.worker.admission.jitter-max");
        nonNegative(admission.genericCooldown, "replicadb.worker.admission.generic-cooldown");
        if (admission.directedQueueCapacity < 1
                || admission.directedQueueCapacity > MAX_DIRECTED_QUEUE_CAPACITY) {
            throw new IllegalArgumentException(
                    "replicadb.worker.admission.directed-queue-capacity must be between 1 and "
                            + MAX_DIRECTED_QUEUE_CAPACITY);
        }
        if (admission.adaptiveBackoff == null) {
            throw new IllegalArgumentException("replicadb.worker.admission.adaptive-backoff must not be null");
        }
        if (!admission.adaptiveBackoff.enabled) {
            return;
        }
        positive(admission.adaptiveBackoff.initialDelay,
                "replicadb.worker.admission.adaptive-backoff.initial-delay");
        positive(admission.adaptiveBackoff.maxDelay,
                "replicadb.worker.admission.adaptive-backoff.max-delay");
        positive(admission.adaptiveBackoff.decayHalfLife,
                "replicadb.worker.admission.adaptive-backoff.decay-half-life");
        if (admission.adaptiveBackoff.maxDelay.compareTo(admission.adaptiveBackoff.initialDelay) < 0) {
            throw new IllegalArgumentException(
                    "replicadb.worker.admission.adaptive-backoff.max-delay must not be less than initial-delay");
        }
    }

    private static void positive(Duration duration, String property) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(property + " must be positive");
        }
    }

    private static void nonNegative(Duration duration, String property) {
        if (duration == null || duration.isNegative()) {
            throw new IllegalArgumentException(property + " must not be negative");
        }
    }

    public static class Listener {

        private Duration initialReconnectDelay = Duration.ofSeconds(1);
        private Duration maxReconnectDelay = Duration.ofSeconds(30);

        public Duration getInitialReconnectDelay() {
            return initialReconnectDelay;
        }

        public void setInitialReconnectDelay(Duration initialReconnectDelay) {
            this.initialReconnectDelay = initialReconnectDelay;
        }

        public Duration getMaxReconnectDelay() {
            return maxReconnectDelay;
        }

        public void setMaxReconnectDelay(Duration maxReconnectDelay) {
            this.maxReconnectDelay = maxReconnectDelay;
        }
    }

    public static class Admission {

        private Duration jitterMax = Duration.ofMillis(100);
        private Duration genericCooldown = Duration.ofMillis(250);
        private int directedQueueCapacity = 1_024;
        private AdaptiveBackoff adaptiveBackoff = new AdaptiveBackoff();

        public Duration getJitterMax() {
            return jitterMax;
        }

        public void setJitterMax(Duration jitterMax) {
            this.jitterMax = jitterMax;
        }

        public Duration getGenericCooldown() {
            return genericCooldown;
        }

        public void setGenericCooldown(Duration genericCooldown) {
            this.genericCooldown = genericCooldown;
        }

        public int getDirectedQueueCapacity() {
            return directedQueueCapacity;
        }

        public void setDirectedQueueCapacity(int directedQueueCapacity) {
            this.directedQueueCapacity = directedQueueCapacity;
        }

        public AdaptiveBackoff getAdaptiveBackoff() {
            return adaptiveBackoff;
        }

        public void setAdaptiveBackoff(AdaptiveBackoff adaptiveBackoff) {
            this.adaptiveBackoff = adaptiveBackoff;
        }
    }

    public static class AdaptiveBackoff {

        private boolean enabled = true;
        private Duration initialDelay = Duration.ofMillis(25);
        private Duration maxDelay = Duration.ofSeconds(2);
        private Duration decayHalfLife = Duration.ofSeconds(30);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public void setMaxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
        }

        public Duration getDecayHalfLife() {
            return decayHalfLife;
        }

        public void setDecayHalfLife(Duration decayHalfLife) {
            this.decayHalfLife = decayHalfLife;
        }
    }
}