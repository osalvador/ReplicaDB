package org.replicadb.server.job.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record JobRun(
        UUID id,
        UUID jobDefinitionId,
        UUID previousRunId,
        JobRunStatus status,
        int attempt,
        String executorIdentity,
        Instant leaseUntil,
        Instant heartbeatAt,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        Long rowsProcessed,
        Long durationMillis,
        String committedWatermark,
        String errorMessage,
        String cancellationWarning,
        Instant availableAt,
        LeaseToken leaseToken) {

    public JobRun {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(availableAt, "availableAt must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1");
        }
    }

    public JobRun(UUID id, UUID jobDefinitionId, UUID previousRunId, JobRunStatus status, int attempt,
                  String executorIdentity, Instant leaseUntil, Instant heartbeatAt, Instant createdAt,
                  Instant startedAt, Instant finishedAt, Long rowsProcessed, Long durationMillis,
                  String committedWatermark, String errorMessage, String cancellationWarning) {
        this(id, jobDefinitionId, previousRunId, status, attempt, executorIdentity, leaseUntil, heartbeatAt,
                createdAt, startedAt, finishedAt, rowsProcessed, durationMillis, committedWatermark,
                errorMessage, cancellationWarning, Objects.requireNonNull(createdAt, "createdAt must not be null"),
                null);
    }
}
