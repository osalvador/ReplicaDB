package org.replicadb.cli;

import org.junit.jupiter.api.Test;
import org.replicadb.execution.ReplicationExecutionContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ToolOptionsExecutionContextTest {

    @Test
    void createsAnExecutionContextForEachOptionsInstance() throws Exception {
        ToolOptions first = options();
        ToolOptions second = options();

        assertNotNull(first.getExecutionContext());
        assertNotSame(first.getExecutionContext(), second.getExecutionContext());
    }

    @Test
    void createsAFreshExecutionContextForEachReplicationTable() throws Exception {
        ToolOptions options = options();
        ReplicationExecutionContext originalContext = options.getExecutionContext();

        ToolOptions tableOptions = options.forReplicationTable(new ReplicationTable("orders", "order_copy"));

        assertNotSame(originalContext, tableOptions.getExecutionContext());
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy"
        });
    }
}
