package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.job.domain.RunLog;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Credential-redacted run diagnostics bounded to 256 KiB of persisted content.")
public record RunLogResponse(
    @Schema(description = "Run identifier.", format = "uuid") UUID runId,
    @Schema(description = "Redacted diagnostic text. Truncated content contains [TRUNCATED: middle omitted].", maxLength = 262144) String content,
    @Schema(description = "Whether captured output exceeded the persisted content bound.") boolean truncated,
    @Schema(description = "Total captured byte count before bounded retention.", minimum = "0") int capturedSize,
    @Schema(description = "Persisted diagnostic format version.", minimum = "1") int formatVersion,
    @Schema(description = "Initial capture timestamp in UTC.", format = "date-time", nullable = true) Instant capturedAt,
    @Schema(description = "Last capture update timestamp in UTC.", format = "date-time", nullable = true) Instant updatedAt) {

    public static RunLogResponse empty(UUID runId) {
        return new RunLogResponse(runId, "", false, 0, RunLog.CURRENT_FORMAT_VERSION, null, null);
    }

    public static RunLogResponse from(RunLog runLog) {
        return new RunLogResponse(runLog.runId(), runLog.content(), runLog.truncated(),
                runLog.capturedSize(), runLog.formatVersion(), runLog.capturedAt(), runLog.updatedAt());
    }
}
