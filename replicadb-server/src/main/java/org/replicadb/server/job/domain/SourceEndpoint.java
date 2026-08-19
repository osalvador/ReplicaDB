package org.replicadb.server.job.domain;

import java.util.Objects;

public record SourceEndpoint(
        ConnectionCredentials connection,
        String table,
        String columns,
        String where,
        String query) {

    public SourceEndpoint {
        Objects.requireNonNull(connection, "connection must not be null");
        if (isBlank(table) && isBlank(query)) {
            throw new IllegalArgumentException("source table or query must be configured");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
