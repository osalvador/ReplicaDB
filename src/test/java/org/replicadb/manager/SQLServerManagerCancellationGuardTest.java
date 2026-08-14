package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SQLServerManagerCancellationGuardTest {

    @Test
    void detectsCancellationBeforeBulkCopyStarts() throws Exception {
        ToolOptions activeOptions = options();
        SQLServerManager activeManager = new SQLServerManager(activeOptions, DataSourceType.SINK);

        assertFalse(activeManager.shouldAbortBeforeBulkCopy());

        activeOptions.getExecutionContext().requestCancellation();

        assertTrue(activeManager.shouldAbortBeforeBulkCopy());
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlserver://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:sqlserver://sink",
                "--sink-table", "customer_copy"
        });
    }
}