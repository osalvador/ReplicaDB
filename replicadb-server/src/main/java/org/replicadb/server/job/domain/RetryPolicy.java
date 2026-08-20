package org.replicadb.server.job.domain;

import org.replicadb.cli.ReplicationMode;

import java.util.Objects;

public record RetryPolicy(int maxAttempts, long retryBackoffSeconds, boolean automaticRetryEnabled) {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_RETRY_BACKOFF_SECONDS = 60;

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (retryBackoffSeconds < 0) {
            throw new IllegalArgumentException("retryBackoffSeconds must not be negative");
        }
    }

    public static RetryPolicy defaultsFor(ReplicationMode mode) {
        Objects.requireNonNull(mode, "mode must not be null");
        return new RetryPolicy(DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_BACKOFF_SECONDS,
                mode != ReplicationMode.COMPLETE);
    }
}
