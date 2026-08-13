package org.replicadb.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplicationTableTest {

    @Test
    void createsQualifiedSourceAndSinkPair() {
        ReplicationTable table = new ReplicationTable("source.orders", "warehouse.sales_orders");

        assertEquals("source.orders", table.sourceTable());
        assertEquals("warehouse.sales_orders", table.sinkTable());
    }

    @Test
    void recordsValueEquality() {
        ReplicationTable first = new ReplicationTable("orders", "orders");
        ReplicationTable second = new ReplicationTable("orders", "orders");

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void rejectsNullOrBlankTableNames() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicationTable(null, "orders"));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationTable(" ", "orders"));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationTable("orders", null));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationTable("orders", "\t"));
    }
}
