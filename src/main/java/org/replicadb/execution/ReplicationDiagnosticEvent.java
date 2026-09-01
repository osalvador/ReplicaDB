package org.replicadb.execution;

import java.time.Instant;
import java.util.Objects;

public record ReplicationDiagnosticEvent(
        Instant timestamp,
        Stage stage,
        Category category,
        Severity severity,
        String taskId,
        String component,
        String message,
        String throwableSummary,
        String stacktrace) {

    public ReplicationDiagnosticEvent {
        timestamp = Objects.requireNonNull(timestamp, "timestamp");
        stage = Objects.requireNonNull(stage, "stage");
        category = Objects.requireNonNull(category, "category");
        severity = Objects.requireNonNull(severity, "severity");
        taskId = normalize(taskId);
        component = required(component, "component");
        message = required(message, "message");
        throwableSummary = normalize(throwableSummary);
        stacktrace = normalize(stacktrace);
    }

    public enum Stage {
        VALIDATION, SOURCE_CONNECTION, SINK_CONNECTION, SOURCE_READ, SINK_WRITE,
        WATERMARK, PRE_TASK, POST_TASK, CANCELLATION, INTERRUPTION, AGGREGATION, CLEANUP
    }

    public enum Category {
        VALIDATION, CONNECTION, READ, WRITE, WATERMARK, LIFECYCLE, CANCELLATION, CLEANUP, FAILURE
    }

    public enum Severity {
        INFO, DEBUG, WARN, ERROR
    }

    private static String required(String value, String field) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
