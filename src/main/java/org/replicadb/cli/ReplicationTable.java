package org.replicadb.cli;

public record ReplicationTable(String sourceTable, String sinkTable) {

    public ReplicationTable {
        requireTableName(sourceTable, "source table");
        requireTableName(sinkTable, "sink table");
    }

    private static void requireTableName(String tableName, String label) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank.");
        }
    }
}
