package org.replicadb.server.job.domain;

import java.util.Objects;
import java.util.UUID;

public record SourceEndpoint(
        UUID datasourceId,
        String table,
        String columns,
        String where,
        String query,
        ConnectionCredentials legacyConnection) {

    public SourceEndpoint {
        if (datasourceId == null && legacyConnection == null) {
            throw new IllegalArgumentException("source datasource must be configured");
        }
        if (isBlank(table) && isBlank(query)) {
            throw new IllegalArgumentException("source table or query must be configured");
        }
    }

    public SourceEndpoint(UUID datasourceId, String table, String columns, String where, String query) {
        this(datasourceId, table, columns, where, query, null);
    }

    /**
     * @deprecated Managed jobs must use a datasource identifier. Kept only while
     *             the API and persistence adapters are migrated.
     */
    @Deprecated
    public SourceEndpoint(ConnectionCredentials connection, String table, String columns,
                          String where, String query) {
        this(null, table, columns, where, query,
                Objects.requireNonNull(connection, "connection must not be null"));
    }

    /**
     * @deprecated Managed jobs must use a datasource identifier.
     */
    @Deprecated
    public ConnectionCredentials connection() {
        return legacyConnection;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
