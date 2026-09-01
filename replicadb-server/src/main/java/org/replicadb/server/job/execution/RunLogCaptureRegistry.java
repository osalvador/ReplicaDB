package org.replicadb.server.job.execution;

import org.replicadb.config.CredentialRedactor;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class RunLogCaptureRegistry {

    private final Map<String, Capture> captures = new ConcurrentHashMap<>();

    public Capture register(String runId) {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        Capture capture = new Capture(runId);
        if (captures.putIfAbsent(runId, capture) != null) {
            throw new IllegalStateException("A log capture is already registered for run " + runId);
        }
        return capture;
    }

    public Capture register(java.util.UUID runId) {
        if (runId == null) {
            throw new IllegalArgumentException("runId must not be null");
        }
        return register(runId.toString());
    }

    public void alias(String runId, Capture capture) {
        if (runId == null || runId.isBlank() || capture == null) {
            throw new IllegalArgumentException("runId and capture are required");
        }
        if (captures.putIfAbsent(runId, capture) != null) {
            throw new IllegalStateException("A log capture is already registered for run " + runId);
        }
    }

    Capture find(String runId) {
        return runId == null ? null : captures.get(runId);
    }

    public void unregister(Capture capture) {
        if (capture != null) {
            captures.entrySet().removeIf(entry -> entry.getValue() == capture);
            capture.close();
        }
    }

    int size() {
        return captures.size();
    }

    public static final class Capture {
        private final String runId;
        private final StringBuilder content = new StringBuilder();
        private boolean closed;
        private boolean truncated;
        private int capturedBytes;

        private Capture(String runId) {
            this.runId = runId;
        }

        public String runId() {
            return runId;
        }

        public synchronized void append(String value) {
            if (closed || value == null || value.isEmpty()) {
                return;
            }
            String redacted = CredentialRedactor.redactMessage(value);
            capturedBytes += redacted.getBytes(StandardCharsets.UTF_8).length;
            String combined = content + redacted;
            content.setLength(0);
            content.append(BoundedText.limit(combined));
            truncated |= !content.toString().equals(combined);
        }

        public synchronized Snapshot snapshot() {
            return new Snapshot(content.toString(), truncated, capturedBytes);
        }

        private synchronized void close() {
            closed = true;
        }
    }

    public record Snapshot(String content, boolean truncated, int capturedBytes) {
    }

    static final class BoundedText {
        static final int MAX_BYTES = 256 * 1024;
        static final String MARKER = "[TRUNCATED: middle omitted]";

        private BoundedText() {
        }

        static String limit(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            if (bytes.length <= MAX_BYTES) {
                return value;
            }
            byte[] marker = MARKER.getBytes(StandardCharsets.UTF_8);
            int available = MAX_BYTES - marker.length;
            int first = available * 3 / 4;
            int last = available - first;
            return decode(bytes, 0, first) + MARKER + decode(bytes, bytes.length - last, last);
        }

        private static String decode(byte[] bytes, int offset, int length) {
            int end = Math.min(bytes.length, offset + length);
            while (offset < end && (bytes[offset] & 0xc0) == 0x80) {
                offset++;
            }
            while (end > offset && end < bytes.length && (bytes[end] & 0xc0) == 0x80) {
                end--;
            }
            return new String(bytes, offset, end - offset, StandardCharsets.UTF_8);
        }
    }
}
