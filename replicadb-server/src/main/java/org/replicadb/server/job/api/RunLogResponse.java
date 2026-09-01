package org.replicadb.server.job.api;

import org.replicadb.server.job.domain.RunLog;

import java.time.Instant;
import java.util.UUID;

public record RunLogResponse(UUID runId, String content, boolean truncated, int capturedSize,
                             int formatVersion, Instant capturedAt, Instant updatedAt) {

    public static RunLogResponse empty(UUID runId) {
        return new RunLogResponse(runId, "", false, 0, RunLog.CURRENT_FORMAT_VERSION, null, null);
    }

    public static RunLogResponse from(RunLog runLog) {
        return new RunLogResponse(runLog.runId(), runLog.content(), runLog.truncated(),
                runLog.capturedSize(), runLog.formatVersion(), runLog.capturedAt(), runLog.updatedAt());
    }
}
