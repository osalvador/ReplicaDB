package org.replicadb.server.job.domain;

import java.util.Objects;

public record SinkEndpoint(
        ConnectionCredentials connection,
        String table,
        String columns,
        StagingOptions staging,
        boolean disableEscape,
        boolean disableTruncate) {

    public SinkEndpoint {
        Objects.requireNonNull(connection, "connection must not be null");
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("sink table must not be blank");
        }
    }
}
