package org.replicadb.server.job.domain;

import org.replicadb.cli.ReplicationMode;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

public record JobDefinition(
        UUID id,
        String name,
        String sourceConnect,
        String sourceUser,
        String sourcePassword,
        String sourceTable,
        String sourceWhere,
        String sinkConnect,
        String sinkUser,
        String sinkPassword,
        String sinkTable,
        ReplicationMode mode,
        int jobs,
        String incrementalWatermarkColumn,
        String initialWatermarkValue,
        Instant createdAt,
        Instant updatedAt) {

    private static final Pattern ENV_REFERENCE = Pattern.compile("\\$\\{env:[A-Za-z_][A-Za-z0-9_]*}");
        private static final Pattern EMBEDDED_CREDENTIAL = Pattern.compile(
            "(?i)(?:password|passwd|pwd|secret|token)\\s*=|://[^/?#;]+:[^/?#;]+@");

    public JobDefinition {
        requireNonBlank("name", name);
        requireNonBlank("sourceConnect", sourceConnect);
        requireNonBlank("sourceTable", sourceTable);
        requireNonBlank("sinkConnect", sinkConnect);
        requireNonBlank("sinkTable", sinkTable);
        validateConnectionReference("sourceConnect", sourceConnect);
        validateConnectionReference("sinkConnect", sinkConnect);
        Objects.requireNonNull(mode, "mode must not be null");
        if (jobs < 1) {
            throw new IllegalArgumentException("jobs must be at least 1");
        }
        if (incrementalWatermarkColumn != null && mode != ReplicationMode.INCREMENTAL) {
            throw new IllegalArgumentException("incrementalWatermarkColumn requires incremental mode");
        }
        validateSecretReference("sourcePassword", sourcePassword);
        validateSecretReference("sinkPassword", sinkPassword);
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void validateSecretReference(String fieldName, String value) {
        if (value != null && !ENV_REFERENCE.matcher(value).matches()) {
            throw new IllegalArgumentException(fieldName
                    + " must be an ${env:VARIABLE} reference");
        }
    }

    private static void validateConnectionReference(String fieldName, String value) {
        if (EMBEDDED_CREDENTIAL.matcher(value).find()) {
            throw new IllegalArgumentException(fieldName + " must not contain embedded credentials");
        }
    }
}