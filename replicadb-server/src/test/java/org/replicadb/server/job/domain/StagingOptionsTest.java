package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StagingOptionsTest {

    @Test
    void storesSchemaAndTable() {
        StagingOptions options = new StagingOptions("staging", "orders_stage");

        assertEquals("staging", options.schema());
        assertEquals("orders_stage", options.table());
    }
}
