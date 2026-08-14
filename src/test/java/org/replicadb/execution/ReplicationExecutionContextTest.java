package org.replicadb.execution;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ReplicationExecutionContextTest {

    @Test
    void initializesRunState() {
        ReplicationExecutionContext context = new ReplicationExecutionContext();

        assertNotNull(context.getRunId());
        assertNotEquals("", context.getRunId());
        assertNull(context.getSinkStagingTableName());
        assertEquals(0, context.getTempFilePathSize());
    }

    @Test
    void storesGeneratedStagingNameAndTempFilePaths() {
        ReplicationExecutionContext context = new ReplicationExecutionContext();

        context.setSinkStagingTableName("orders_staging");
        context.setTempFilePath(0, "/tmp/orders-0.csv");
        context.setTempFilePath(1, "/tmp/orders-1.csv");

        assertEquals("orders_staging", context.getSinkStagingTableName());
        assertEquals("/tmp/orders-0.csv", context.getTempFilePath(0));
        assertEquals("/tmp/orders-1.csv", context.getTempFilePath(1));
        assertEquals(2, context.getTempFilePathSize());
        assertEquals(Map.of(0, "/tmp/orders-0.csv", 1, "/tmp/orders-1.csv"),
                context.getTempFilesPath());
    }

    @Test
    void givesEachContextAUniqueRunId() {
        ReplicationExecutionContext first = new ReplicationExecutionContext();
        ReplicationExecutionContext second = new ReplicationExecutionContext();

        assertNotEquals(first.getRunId(), second.getRunId());
    }
}
