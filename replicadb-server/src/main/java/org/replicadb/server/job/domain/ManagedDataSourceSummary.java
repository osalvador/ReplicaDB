package org.replicadb.server.job.domain;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record ManagedDataSourceSummary(
        UUID id,
        String name,
        ConnectorType connectorType,
        String safeConnectDisplay,
        Map<String, String> technicalParams,
        boolean securityConfigured,
        int securityFormatVersion,
        String encryptionAlgorithm,
        String keyVersion,
        Instant createdAt,
        Instant updatedAt) {

    public ManagedDataSourceSummary {
        Objects.requireNonNull(id, "id must not be null");
        requireNonBlank("name", name);
        Objects.requireNonNull(connectorType, "connectorType must not be null");
        requireNonBlank("safeConnectDisplay", safeConnectDisplay);
        technicalParams = technicalParams == null ? Map.of() : Map.copyOf(technicalParams);
        if (securityFormatVersion < 1) {
            throw new IllegalArgumentException("securityFormatVersion must be positive");
        }
        requireNonBlank("encryptionAlgorithm", encryptionAlgorithm);
        requireNonBlank("keyVersion", keyVersion);
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
