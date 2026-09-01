package org.replicadb.server.job.domain;

import java.util.Objects;
import java.util.UUID;

public record SinkEndpoint(
        UUID datasourceId,
        String table,
        String columns,
        StagingOptions staging,
        boolean disableEscape,
        boolean disableTruncate,
        ConnectionCredentials legacyConnection) {

    public SinkEndpoint {
        if (datasourceId == null && legacyConnection == null) {
            throw new IllegalArgumentException("sink datasource must be configured");
        }
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("sink table must not be blank");
        }
    }

    public SinkEndpoint(UUID datasourceId, String table, String columns, StagingOptions staging,
                        boolean disableEscape, boolean disableTruncate) {
        this(datasourceId, table, columns, staging, disableEscape, disableTruncate, null);
    }

    /**
     * @deprecated Managed jobs must use a datasource identifier. Kept only while
     *             the API and persistence adapters are migrated.
     */
    @Deprecated
    public SinkEndpoint(ConnectionCredentials connection, String table, String columns,
                        StagingOptions staging, boolean disableEscape, boolean disableTruncate) {
        this(null, table, columns, staging, disableEscape, disableTruncate,
                Objects.requireNonNull(connection, "connection must not be null"));
    }

    /**
     * @deprecated Managed jobs must use a datasource identifier.
     */
    @Deprecated
    public ConnectionCredentials connection() {
        return legacyConnection;
    }
}
