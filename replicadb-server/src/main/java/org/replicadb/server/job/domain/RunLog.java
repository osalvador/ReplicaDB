package org.replicadb.server.job.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record RunLog(UUID runId, String content, boolean truncated, int capturedSize,
                     int formatVersion, Instant capturedAt, Instant updatedAt) {

    public static final int MAX_BYTES = 256 * 1024;
    public static final int CURRENT_FORMAT_VERSION = 1;

    public RunLog {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(content, "content must not be null");
        Objects.requireNonNull(capturedAt, "capturedAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        int contentBytes = content.getBytes(StandardCharsets.UTF_8).length;
        if (contentBytes > MAX_BYTES) {
            throw new IllegalArgumentException("content exceeds the run-log byte limit");
        }
        if (capturedSize < contentBytes) {
            throw new IllegalArgumentException("capturedSize must include content bytes");
        }
        if (formatVersion < 1) {
            throw new IllegalArgumentException("formatVersion must be positive");
        }
    }
}
