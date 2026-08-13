package org.replicadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;
import org.replicadb.manager.file.FileManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaDBMultipleTablesTest {

    @Test
    void executesEachTableInOrderAndCleansItBeforeCreatingTheNextOne() throws Exception {
        ToolOptions options = multiTableOptions("complete");
        RecordingManagerFactory managerFactory = new RecordingManagerFactory();

        assertEquals(0, ReplicaDB.processReplica(options, managerFactory));

        int secondTableCreation = indexOfEvent(managerFactory.events, "create:orders");
        int firstTableCompletion = lastIndexOfEventContaining(managerFactory.events, "customer_copy");
        assertTrue(secondTableCreation > firstTableCompletion);
        assertEquals(List.of("customers", "customer_copy", "customers", "customer_copy",
                "orders", "order_copy", "orders", "order_copy"),
                managerFactory.createdTableNames());
        assertTrue(managerFactory.events.contains("pre-source:customers"));
        assertTrue(managerFactory.events.contains("pre-sink:customer_copy"));
        assertTrue(managerFactory.events.contains("post-source:customers"));
        assertTrue(managerFactory.events.contains("post-sink:customer_copy"));
        assertTrue(managerFactory.events.contains("cleanup:customer_copy"));
    }

    @Test
    void stopsAfterTheFirstFailedTable() throws Exception {
        ToolOptions options = multiTableOptions("complete");
        options = addThirdTable(options);
        RecordingManagerFactory managerFactory = new RecordingManagerFactory("orders");

        assertEquals(1, ReplicaDB.processReplica(options, managerFactory));

        assertFalse(managerFactory.events.stream().anyMatch(event -> event.contains("products")));
        assertTrue(managerFactory.events.contains("close:customers"));
        assertTrue(managerFactory.events.contains("close:order_copy"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"complete", "incremental", "complete-atomic"})
    void runsHooksForEachSupportedMode(String mode) throws Exception {
        ToolOptions options = multiTableOptions(mode);
        RecordingManagerFactory managerFactory = new RecordingManagerFactory();

        assertEquals(0, ReplicaDB.processReplica(options, managerFactory));

        assertEquals(2, countEvents(managerFactory.events, "pre-source:"));
        assertEquals(2, countEvents(managerFactory.events, "pre-sink:"));
        assertEquals(2, countEvents(managerFactory.events, "post-source:"));
        assertEquals(2, countEvents(managerFactory.events, "post-sink:"));
    }

    @Test
    void resetsStagingAndTemporaryStateAtEachTableBoundary() throws Exception {
        ToolOptions options = multiTableOptions("incremental");
        FileManager.setTempFilesPath(new HashMap<>(Map.of(7, "stale-file")));
        RecordingManagerFactory managerFactory = new RecordingManagerFactory();

        assertEquals(0, ReplicaDB.processReplica(options, managerFactory));

        assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0), managerFactory.tempFileSizes);
        assertEquals(8, managerFactory.stagingNames.size());
        assertEquals(1, managerFactory.stagingNames.subList(0, 4).stream().distinct().count());
        assertEquals(1, managerFactory.stagingNames.subList(4, 8).stream().distinct().count());
        assertNotEquals(managerFactory.stagingNames.get(0), managerFactory.stagingNames.get(4));
    }

    @Test
    void attemptsAllMainManagerCleanupOperationsWhenSinkCleanupFails() throws Exception {
        ToolOptions options = multiTableOptions("complete");
        RecordingManagerFactory managerFactory = new RecordingManagerFactory();
        managerFactory.failSinkCleanup = true;

        assertEquals(0, ReplicaDB.processReplica(options, managerFactory));

        assertTrue(managerFactory.events.contains("cleanup:customer_copy"));
        assertTrue(managerFactory.events.contains("close:customer_copy"));
        assertTrue(managerFactory.events.contains("close:customers"));
    }

    @Test
    void preservesTheLegacySingleTablePath() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "legacy_source",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "legacy_sink",
                "--jobs", "1"
        });
        RecordingManagerFactory managerFactory = new RecordingManagerFactory();

        assertEquals(0, ReplicaDB.processReplica(options, managerFactory));

        assertFalse(options.hasReplicationTables());
        assertEquals(List.of("legacy_source", "legacy_sink", "legacy_source", "legacy_sink"),
            managerFactory.createdTableNames());
    }

    @Test
    void helpDoesNotCreateManagers() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{"--help"});
        RecordingManagerFactory managerFactory = new RecordingManagerFactory();

        assertEquals(0, ReplicaDB.processReplica(options, managerFactory));

        assertTrue(managerFactory.createdTableNames().isEmpty());
    }

    private static ToolOptions multiTableOptions(String mode) throws Exception {
        Path optionsFile = Files.createTempFile("replicadb-multiple-tables-", ".properties");
        Files.writeString(optionsFile, String.join(System.lineSeparator(),
                "mode=" + mode,
                "jobs=1",
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink",
                "replication.table.1.source=customers",
                "replication.table.1.sink=customer_copy",
                "replication.table.2.source=orders",
                "replication.table.2.sink=order_copy"));
        optionsFile.toFile().deleteOnExit();
        return new ToolOptions(new String[]{"--options-file", optionsFile.toString()});
    }

    private static ToolOptions addThirdTable(ToolOptions options) throws IOException {
        Path optionsFile = Files.createTempFile("replicadb-three-tables-", ".properties");
        Files.writeString(optionsFile, String.join(System.lineSeparator(),
                "mode=" + options.getMode(),
                "jobs=1",
                "source.connect=" + options.getSourceConnect(),
                "sink.connect=" + options.getSinkConnect(),
                "replication.table.1.source=customers",
                "replication.table.1.sink=customer_copy",
                "replication.table.2.source=orders",
                "replication.table.2.sink=order_copy",
                "replication.table.3.source=products",
                "replication.table.3.sink=product_copy"));
        optionsFile.toFile().deleteOnExit();
        try {
            return new ToolOptions(new String[]{"--options-file", optionsFile.toString()});
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static int indexOfEvent(List<String> events, String eventPart) {
        for (int index = 0; index < events.size(); index++) {
            if (events.get(index).contains(eventPart)) {
                return index;
            }
        }
        return -1;
    }

    private static int lastIndexOfEventContaining(List<String> events, String eventPart) {
        for (int index = events.size() - 1; index >= 0; index--) {
            if (events.get(index).contains(eventPart)) {
                return index;
            }
        }
        return -1;
    }

    private static int countEvents(List<String> events, String eventPrefix) {
        return (int) events.stream().filter(event -> event.startsWith(eventPrefix)).count();
    }

    private static final class RecordingManagerFactory extends ManagerFactory {
        private final List<String> events = new ArrayList<>();
        private final List<String> stagingNames = new ArrayList<>();
        private final List<Integer> tempFileSizes = new ArrayList<>();
        private final String failingSourceTable;
        private boolean failSinkCleanup;

        private RecordingManagerFactory() {
            this(null);
        }

        private RecordingManagerFactory(String failingSourceTable) {
            this.failingSourceTable = failingSourceTable;
        }

        @Override
        public void validateAzureAuthenticationConfiguration(ToolOptions options) {
        }

        @Override
        public ConnManager accept(ToolOptions options, DataSourceType dataSourceType) {
            String tableName = DataSourceType.SOURCE.equals(dataSourceType)
                    ? options.getSourceTable()
                    : options.getSinkTable();
                tempFileSizes.add(FileManager.getTempFilePathSize());
            events.add("create:" + tableName);
                RecordingManager manager = new RecordingManager(options, dataSourceType, events,
                    DataSourceType.SOURCE.equals(dataSourceType)
                            && tableName.equals(failingSourceTable),
                        DataSourceType.SINK.equals(dataSourceType) && failSinkCleanup);
                if (!"complete".equals(options.getMode())) {
                stagingNames.add(manager.getSinkStagingTableName());
                }
                return manager;
        }

        private List<String> createdTableNames() {
            return events.stream()
                    .filter(event -> event.startsWith("create:"))
                    .map(event -> event.substring("create:".length()))
                    .toList();
        }
    }

    private static final class RecordingManager extends ConnManager {
        private final ToolOptions options;
        private final DataSourceType dataSourceType;
        private final List<String> events;
        private final boolean failOnConnection;
        private final boolean failOnCleanup;

        private RecordingManager(ToolOptions options, DataSourceType dataSourceType,
                                 List<String> events, boolean failOnConnection, boolean failOnCleanup) {
            super.options = options;
            this.options = options;
            this.dataSourceType = dataSourceType;
            this.events = events;
            this.failOnConnection = failOnConnection;
            this.failOnCleanup = failOnCleanup;
        }

        @Override
        public ResultSet readTable(String tableName, String[] columns, int nThread) {
            return null;
        }

        @Override
        public int insertDataToTable(ResultSet resultSet, int taskId) {
            return 0;
        }

        @Override
        public Connection getConnection() {
            if (failOnConnection) {
                throw new IllegalStateException("replication failure");
            }
            return null;
        }

        @Override
        public String getDriverClass() {
            return "";
        }

        @Override
        public void close() {
            events.add("close:" + tableName());
        }

        @Override
        public void cleanUp() {
            events.add("cleanup:" + tableName());
            if (failOnCleanup) {
                throw new IllegalStateException("sink cleanup failed");
            }
        }

        @Override
        public void release() {
        }

        @Override
        public Future<Integer> preSinkTasks(ExecutorService executor) {
            events.add("pre-sink:" + tableName());
            return null;
        }

        @Override
        public void preSourceTasks() {
            events.add("pre-source:" + tableName());
        }

        @Override
        public void postSourceTasks() {
            events.add("post-source:" + tableName());
        }

        @Override
        public void postSinkTasks() {
            events.add("post-sink:" + tableName());
        }

        @Override
        public String[] getSinkPrimaryKeys(String tableName) {
            return new String[0];
        }

        private String tableName() {
            return DataSourceType.SOURCE.equals(dataSourceType)
                    ? options.getSourceTable()
                    : options.getSinkTable();
        }
    }
}
