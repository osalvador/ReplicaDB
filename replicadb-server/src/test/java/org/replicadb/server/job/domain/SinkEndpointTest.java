package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SinkEndpointTest {

    private final ConnectionCredentials connection =
            new ConnectionCredentials("jdbc:sink", null, null, null, null);

    @Test
    void acceptsTableWithoutStaging() {
        assertDoesNotThrow(() -> new SinkEndpoint(connection, "sink_table", null, null, false, false));
    }

    @Test
    void acceptsStagingOptions() {
        assertDoesNotThrow(() -> new SinkEndpoint(connection, "sink_table", "id, name",
                new StagingOptions("staging", "sink_stage"), true, true));
    }

    @Test
    void rejectsBlankTable() {
        assertThrows(IllegalArgumentException.class,
                () -> new SinkEndpoint(connection, " ", null, null, false, false));
    }
}
