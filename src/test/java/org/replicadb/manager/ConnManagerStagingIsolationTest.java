package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ConnManagerStagingIsolationTest {

    @Test
    void generatedStagingNameIsSharedWithinRunButResetBetweenTables() throws Exception {
        ConnManager.resetGeneratedSinkStagingTableName();
        StagingManager first = new StagingManager(options("customers", "customer_copy"));
        StagingManager firstTask = new StagingManager(options("customers", "customer_copy"));

        String firstName = first.getSinkStagingTableName();

        assertEquals(firstName, firstTask.getSinkStagingTableName());

        ConnManager.resetGeneratedSinkStagingTableName();
        StagingManager second = new StagingManager(options("orders", "order_copy"));

        assertNotEquals(firstName, second.getSinkStagingTableName());
        assertEquals(second.getSinkStagingTableName(),
                new StagingManager(options("orders", "order_copy")).getSinkStagingTableName());
    }

    @Test
    void userDefinedStagingNameIsNotReplacedByReset() throws Exception {
        StagingManager manager = new StagingManager(options("customers", "customer_copy", "custom_staging"));

        ConnManager.resetGeneratedSinkStagingTableName();

        assertEquals("custom_staging", manager.getSinkStagingTableName());
    }

    private static ToolOptions options(String sourceTable, String sinkTable, String... stagingTable)
            throws Exception {
        String[] args = {
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", sourceTable,
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", sinkTable
        };
        if (stagingTable.length == 0) {
            return new ToolOptions(args);
        }

        String[] argsWithStaging = new String[args.length + 2];
        System.arraycopy(args, 0, argsWithStaging, 0, args.length);
        argsWithStaging[args.length] = "--sink-staging-table";
        argsWithStaging[args.length + 1] = stagingTable[0];
        return new ToolOptions(argsWithStaging);
    }

    private static final class StagingManager extends ConnManager {

        private StagingManager(ToolOptions options) {
            this.options = options;
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
            return null;
        }

        @Override
        public String getDriverClass() {
            return "";
        }

        @Override
        public void close() {
        }

        @Override
        public void cleanUp() {
        }

        @Override
        public void release() {
        }

        @Override
        public void preSourceTasks() {
        }

        @Override
        public Future<Integer> preSinkTasks(ExecutorService executor) {
            return null;
        }

        @Override
        public void postSourceTasks() {
        }

        @Override
        public void postSinkTasks() {
        }

        @Override
        public String[] getSinkPrimaryKeys(String tableName) {
            return new String[0];
        }
    }
}