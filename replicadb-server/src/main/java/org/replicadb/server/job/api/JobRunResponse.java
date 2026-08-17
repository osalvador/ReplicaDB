package org.replicadb.server.job.api;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;

import java.time.Instant;
import java.util.UUID;

public record JobRunResponse(
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
        String cancellationWarning) {

    public static JobRunResponse from(JobRun run) {
        return new JobRunResponse(
                run.id(), run.jobDefinitionId(), run.previousRunId(), run.status(), run.attempt(),
                run.executorIdentity(), run.leaseUntil(), run.heartbeatAt(), run.createdAt(),
                run.startedAt(), run.finishedAt(), run.rowsProcessed(), run.durationMillis(),
                run.committedWatermark(), run.errorMessage(), run.cancellationWarning());
    }
}
