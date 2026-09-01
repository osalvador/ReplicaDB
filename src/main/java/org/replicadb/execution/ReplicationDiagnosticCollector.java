package org.replicadb.execution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.replicadb.config.CredentialRedactor;

public interface ReplicationDiagnosticCollector {

    int MAX_BYTES = 256 * 1024;
    String TRUNCATION_MARKER = "[TRUNCATED: middle omitted]";

    void record(ReplicationDiagnosticEvent event);

    Snapshot snapshot();

    default void record(ReplicationDiagnosticEvent.Stage stage,
                        ReplicationDiagnosticEvent.Category category,
                        ReplicationDiagnosticEvent.Severity severity,
                        String taskId, String component, String message, Throwable throwable) {
        String summary = throwable == null ? null : CredentialRedactor.redactMessage(throwable.toString());
        String stacktrace = null;
        if (throwable != null) {
            StringWriter writer = new StringWriter();
            throwable.printStackTrace(new PrintWriter(writer));
            stacktrace = CredentialRedactor.redactMessage(writer.toString());
        }
        record(new ReplicationDiagnosticEvent(Instant.now(), stage, category, severity,
                taskId, component, message, summary, stacktrace));
    }

    record Snapshot(List<ReplicationDiagnosticEvent> events, String content,
                    boolean truncated, int capturedBytes) {
        public Snapshot {
            events = Collections.unmodifiableList(new ArrayList<>(events));
            content = content == null ? "" : content;
            capturedBytes = Math.max(0, capturedBytes);
        }
    }

    final class Bounded implements ReplicationDiagnosticCollector {
        private final List<ReplicationDiagnosticEvent> events = new ArrayList<>();
        private String content = "";
        private boolean truncated;
        private int capturedBytes;

        @Override
        public synchronized void record(ReplicationDiagnosticEvent event) {
            if (event == null) {
                throw new NullPointerException("event");
            }
            events.add(event);
            String rendered = render(event);
            capturedBytes = Math.addExact(capturedBytes,
                    rendered.getBytes(StandardCharsets.UTF_8).length);
            String unbounded = content + rendered;
            content = bounded(unbounded);
            truncated = !content.equals(unbounded);
        }

        @Override
        public synchronized Snapshot snapshot() {
            return new Snapshot(events, content, truncated, capturedBytes);
        }

        private static String render(ReplicationDiagnosticEvent event) {
            StringBuilder line = new StringBuilder()
                    .append(event.timestamp()).append(' ')
                    .append(event.severity()).append(' ')
                    .append(event.stage()).append(' ')
                    .append(event.component());
            if (event.taskId() != null) {
                line.append(" [task=").append(event.taskId()).append(']');
            }
            line.append(": ").append(event.message());
            if (event.throwableSummary() != null) {
                line.append("\n").append(event.throwableSummary());
            }
            if (event.stacktrace() != null) {
                line.append("\n").append(event.stacktrace());
            }
            return line.append('\n').toString();
        }

        private static String bounded(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            if (bytes.length <= MAX_BYTES) {
                return value;
            }
            byte[] marker = TRUNCATION_MARKER.getBytes(StandardCharsets.UTF_8);
            int available = MAX_BYTES - marker.length;
            int first = available * 3 / 4;
            int last = available - first;
            return decode(bytes, 0, first) + TRUNCATION_MARKER
                    + decode(bytes, bytes.length - last, last);
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
