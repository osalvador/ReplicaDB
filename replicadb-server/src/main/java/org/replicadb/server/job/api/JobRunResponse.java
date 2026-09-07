package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Public durable run state and attempt lineage. Lease tokens and resolved datasource security are never exposed.")
public record JobRunResponse(
    @Schema(description = "Run attempt identifier.", format = "uuid") UUID id,
    @Schema(description = "Owning job definition identifier.", format = "uuid") UUID jobDefinitionId,
    @Schema(description = "Previous attempt in the retry chain.", format = "uuid", nullable = true) UUID previousRunId,
    @Schema(description = "Durable lifecycle state.") JobRunStatus status,
    @Schema(description = "One-based attempt number.", minimum = "1") int attempt,
    @Schema(description = "Worker identity assigned after claim.", nullable = true) String executorIdentity,
    @Schema(description = "Current claim expiry timestamp in UTC.", format = "date-time", nullable = true) Instant leaseUntil,
    @Schema(description = "Most recent successful worker heartbeat in UTC.", format = "date-time", nullable = true) Instant heartbeatAt,
    @Schema(description = "Run creation timestamp in UTC.", format = "date-time") Instant createdAt,
    @Schema(description = "Earliest database time at which this attempt is eligible for claim.", format = "date-time") Instant availableAt,
    @Schema(description = "Execution start timestamp in UTC.", format = "date-time", nullable = true) Instant startedAt,
    @Schema(description = "Terminal completion timestamp in UTC.", format = "date-time", nullable = true) Instant finishedAt,
    @Schema(description = "Rows processed by this attempt when reported.", minimum = "0", nullable = true) Long rowsProcessed,
    @Schema(description = "Elapsed execution time in milliseconds when reported.", minimum = "0", nullable = true) Long durationMillis,
    @Schema(description = "Incremental watermark committed only by successful finalization.", nullable = true) String committedWatermark,
    @Schema(description = "Credential-redacted failure detail.", nullable = true) String errorMessage,
    @Schema(description = "Mode-specific warning about sink state after cancellation.", nullable = true) String cancellationWarning) {

    public static JobRunResponse from(JobRun run) {
        return new JobRunResponse(
                run.id(), run.jobDefinitionId(), run.previousRunId(), run.status(), run.attempt(),
                run.executorIdentity(), run.leaseUntil(), run.heartbeatAt(), run.createdAt(),
                run.availableAt(), run.startedAt(), run.finishedAt(), run.rowsProcessed(), run.durationMillis(),
                run.committedWatermark(), run.errorMessage(), run.cancellationWarning());
    }
}
