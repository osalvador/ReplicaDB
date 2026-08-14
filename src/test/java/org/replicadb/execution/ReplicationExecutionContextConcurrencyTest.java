package org.replicadb.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ReplicationExecutionContextConcurrencyTest {

    @Test
    void isolatesConcurrentRunsAcrossOneHundredIterations() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < 100; iteration++) {
                ToolOptions firstOptions = options("customer_copy");
                ToolOptions secondOptions = options("order_copy");
                CyclicBarrier barrier = new CyclicBarrier(2);

                Future<RunSnapshot> firstRun = executor.submit(
                        () -> executeRun(firstOptions, barrier, "first"));
                Future<RunSnapshot> secondRun = executor.submit(
                        () -> executeRun(secondOptions, barrier, "second"));

                RunSnapshot firstSnapshot = firstRun.get();
                RunSnapshot secondSnapshot = secondRun.get();

                assertNotEquals(firstSnapshot.stagingTableName(), secondSnapshot.stagingTableName());
                assertEquals(Map.of(
                        0, "first-0",
                        1, "first-1",
                        2, "first-2"), firstOptions.getExecutionContext().getTempFilesPath());
                assertEquals(Map.of(
                        0, "second-0",
                        1, "second-1",
                        2, "second-2"), secondOptions.getExecutionContext().getTempFilesPath());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static RunSnapshot executeRun(ToolOptions options, CyclicBarrier barrier, String pathPrefix)
            throws InterruptedException, BrokenBarrierException {
        barrier.await();
        ConnManager manager = new StagingManager(options);
        String stagingTableName = manager.getSinkStagingTableName();
        for (int taskId = 0; taskId < 3; taskId++) {
            options.getExecutionContext().setTempFilePath(taskId, pathPrefix + "-" + taskId);
        }
        return new RunSnapshot(stagingTableName);
    }

    private static ToolOptions options(String sinkTable) throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", sinkTable
        });
    }

    private record RunSnapshot(String stagingTableName) {
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
        public java.util.concurrent.Future<Integer> preSinkTasks(ExecutorService executor) {
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
