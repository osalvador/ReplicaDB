package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SourceEndpointTest {

    private final ConnectionCredentials connection =
            new ConnectionCredentials("jdbc:source", null, null, null, null);

    @Test
    void acceptsTableOnly() {
        assertDoesNotThrow(() -> new SourceEndpoint(connection, "source_table", null, null, null));
    }

    @Test
    void acceptsQueryOnly() {
        assertDoesNotThrow(() -> new SourceEndpoint(connection, null, null, null, "select 1"));
    }

    @Test
    void acceptsBothTableAndQuery() {
        assertDoesNotThrow(() -> new SourceEndpoint(connection, "source_table", null, null, "select 1"));
    }

    @Test
    void rejectsMissingTableAndQuery() {
        assertThrows(IllegalArgumentException.class,
                () -> new SourceEndpoint(connection, " ", null, null, ""));
    }
}
