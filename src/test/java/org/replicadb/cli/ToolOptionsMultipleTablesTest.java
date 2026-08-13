package org.replicadb.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.replicadb.manager.util.ColumnDescriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ToolOptionsMultipleTablesTest {

    @Test
    void parsesReplicationTablesInNumericOrderAndExpandsEnvironment(@TempDir Path tempDir) throws Exception {
        Path optionsFile = writeOptions(tempDir,
                "replication.table.2.source=orders",
                "replication.table.2.sink=sales_orders",
                "replication.table.1.source=${HOME}.customers",
                "replication.table.1.sink=customers",
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink");

        ToolOptions options = new ToolOptions(new String[]{"--options-file", optionsFile.toString()});

        assertTrue(options.hasReplicationTables());
        assertEquals(List.of(
                        new ReplicationTable(System.getenv("HOME") + ".customers", "customers"),
                        new ReplicationTable("orders", "sales_orders")),
                options.getReplicationTables());
    }

    @Test
    void acceptsLegacySingleTableConfiguration(@TempDir Path tempDir) throws Exception {
        Path optionsFile = writeOptions(tempDir,
                "source.connect=jdbc:postgresql://source",
                "source.table=customers",
                "sink.connect=jdbc:postgresql://sink",
                "sink.table=customers");

        ToolOptions options = new ToolOptions(new String[]{"--options-file", optionsFile.toString()});

        assertFalse(options.hasReplicationTables());
        assertEquals("customers", options.getSourceTable());
        assertEquals("customers", options.getSinkTable());
    }

    @Test
    void rejectsMissingIndex(@TempDir Path tempDir) throws Exception {
        assertInvalid(tempDir,
                "replication.table.1.source=customers",
                "replication.table.1.sink=customers",
                "replication.table.3.source=orders",
                "replication.table.3.sink=orders");
    }

    @Test
    void rejectsMissingPairSide(@TempDir Path tempDir) throws Exception {
        assertInvalid(tempDir,
                "replication.table.1.source=customers");
        assertInvalid(tempDir,
                "replication.table.1.sink=customers");
    }

    @Test
    void rejectsBlankValues(@TempDir Path tempDir) throws Exception {
        assertInvalid(tempDir,
                "replication.table.1.source= ",
                "replication.table.1.sink=customers");
    }

    @Test
    void rejectsInvalidIndexes(@TempDir Path tempDir) throws Exception {
        assertInvalid(tempDir,
                "replication.table.0.source=customers",
                "replication.table.0.sink=customers");
        assertInvalid(tempDir,
                "replication.table.2147483648.source=customers",
                "replication.table.2147483648.sink=customers");
    }

    @Test
    void rejectsMalformedReplicationTableKeys(@TempDir Path tempDir) throws Exception {
        assertInvalid(tempDir,
                "replication.table.1.sorce=customers",
                "replication.table.1.sink=customers");
        assertInvalid(tempDir,
                "replication.table.1.source.extra=customers",
                "replication.table.1.sink=customers");
        assertInvalid(tempDir,
                "replication.table.1 .source=customers",
                "replication.table.1.sink=customers");
    }

        @Test
        void rejectsDuplicateReplicationTableProperties(@TempDir Path tempDir) throws Exception {
                assertInvalid(tempDir,
                                "replication.table.1.source=customers",
                                "replication.table.1.source=other_customers",
                                "replication.table.1.sink=customers");
        }

    @Test
    void rejectsScalarTableOptions(@TempDir Path tempDir) throws Exception {
        Path optionsFile = writeOptions(tempDir,
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink",
                "source.table=legacy_source",
                "sink.table=legacy_sink",
                "replication.table.1.source=customers",
                "replication.table.1.sink=customers");

        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptions(new String[]{"--options-file", optionsFile.toString()}));
    }

    @Test
    void rejectsCliTableOptions(@TempDir Path tempDir) throws Exception {
        Path optionsFile = writeOptions(tempDir,
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink",
                "replication.table.1.source=customers",
                "replication.table.1.sink=customers");

        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptions(new String[]{
                        "--options-file", optionsFile.toString(),
                        "--source-table", "override"}));
    }

    @Test
    void rejectsSourceQueryWithReplicationTables(@TempDir Path tempDir) throws Exception {
        Path optionsFile = writeOptions(tempDir,
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink",
                "source.query=select * from customers",
                "replication.table.1.source=customers",
                "replication.table.1.sink=customers");

        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptions(new String[]{"--options-file", optionsFile.toString()}));
    }

    @Test
    void rejectsFixedStagingTableForIncrementalReplication(@TempDir Path tempDir) throws Exception {
        Path optionsFile = writeOptions(tempDir,
                "mode=incremental",
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink",
                "sink.staging.table=shared_staging",
                "replication.table.1.source=customers",
                "replication.table.1.sink=customers");

        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptions(new String[]{"--options-file", optionsFile.toString()}));
    }

        @Test
        void copiesCommonOptionsAndIsolatesMutableState(@TempDir Path tempDir) throws Exception {
                Path optionsFile = writeOptions(tempDir,
                                "mode=complete",
                                "jobs=2",
                                "fetch.size=50",
                                "source.connect=jdbc:postgresql://source",
                                "source.where=id > 0",
                                "sink.connect=jdbc:postgresql://sink",
                                "sink.staging.schema=staging",
                                "source.file.format=csv",
                                "sink.file.format=csv",
                                "replication.table.1.source=customers",
                                "replication.table.1.sink=customer_copy",
                                "replication.table.2.source=orders",
                                "replication.table.2.sink=order_copy");
                ToolOptions base = new ToolOptions(new String[]{"--options-file", optionsFile.toString()});

                Properties sourceParams = new Properties();
                sourceParams.setProperty("driver", "source-driver");
                base.setSourceConnectionParams(sourceParams);
                base.setSourceAuthMode("ActiveDirectoryManagedIdentity");
                base.setSourceAuthPrincipalId("source-principal");
                base.setSourceColumnDescriptors(List.of(new ColumnDescriptor("id", 4, 10, 0, 1)));
                base.setSourcePrimaryKeys(new String[]{"id"});

                ToolOptions first = base.forReplicationTable(base.getReplicationTables().get(0));
                ToolOptions second = base.forReplicationTable(base.getReplicationTables().get(1));

                assertEquals("customers", first.getSourceTable());
                assertEquals("customer_copy", first.getSinkTable());
                assertEquals("orders", second.getSourceTable());
                assertEquals("order_copy", second.getSinkTable());
                assertEquals("id > 0", first.getSourceWhere());
                assertEquals("staging", first.getSinkStagingSchema());
                assertEquals(2, first.getJobs());
                assertEquals(50, first.getFetchSize());
                assertEquals("csv", first.getSourceFileFormat());
                assertEquals("csv", first.getSinkFileFormat());
                assertFalse(first.hasReplicationTables());
                assertFalse(second.hasReplicationTables());
                assertEquals("source-driver", first.getSourceConnectionParams().getProperty("driver"));
                assertEquals("source-driver", second.getSourceConnectionParams().getProperty("driver"));
                assertEquals(1, first.getSourceColumnDescriptors().size());
                assertArrayEquals(new String[]{"id"}, first.getSourcePrimaryKeys());

                first.getSourceConnectionParams().setProperty("driver", "changed-driver");
                first.getSourcePrimaryKeys()[0] = "changed-id";
                first.setSourceAuthPrincipalId("changed-principal");

                assertEquals("source-driver", base.getSourceConnectionParams().getProperty("driver"));
                assertEquals("source-driver", second.getSourceConnectionParams().getProperty("driver"));
        assertArrayEquals(new String[]{"id"}, base.getSourcePrimaryKeys());
        assertArrayEquals(new String[]{"id"}, second.getSourcePrimaryKeys());
        assertEquals("source-principal", base.getSourceAuthentication().getPrincipalId());
        assertEquals("source-principal", second.getSourceAuthentication().getPrincipalId());
        assertNull(base.getSinkAuthentication().getPrincipalId());
        }

    private static void assertInvalid(Path tempDir, String... tableProperties) throws IOException {
        Path optionsFile = writeOptions(tempDir, mergeRequiredProperties(tableProperties));

        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptions(new String[]{"--options-file", optionsFile.toString()}));
    }

    private static String[] mergeRequiredProperties(String... tableProperties) {
        String[] properties = new String[tableProperties.length + 2];
        properties[0] = "source.connect=jdbc:postgresql://source";
        properties[1] = "sink.connect=jdbc:postgresql://sink";
        System.arraycopy(tableProperties, 0, properties, 2, tableProperties.length);
        return properties;
    }

    private static Path writeOptions(Path tempDir, String... lines) throws IOException {
        Path optionsFile = tempDir.resolve("replicadb.properties");
        Files.writeString(optionsFile, String.join(System.lineSeparator(), lines));
        return optionsFile;
    }
}