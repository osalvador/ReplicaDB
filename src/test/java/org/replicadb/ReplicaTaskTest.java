package org.replicadb;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaTaskTest {

    @Test
    void succeedsAndReturnsRichResultWithRowCountAndTimings() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager();
        RecordingManager sink = new RecordingManager();
        sink.processedRows = 42;

        ReplicaTaskResult result = new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call();

        assertEquals(0, result.taskId());
        assertEquals(42, result.rowsProcessed());
        assertTrue(result.finishedAtMillis() >= result.startedAtMillis());
        assertTrue(result.durationMillis() >= 0);
        assertNull(result.watermarkCandidate());
    }

    @Test
    void succeedsAndReturnsWatermarkCandidateWhenSourceResolvesOne() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager();
        source.watermarkCandidate = "42";
        RecordingManager sink = new RecordingManager();
        sink.processedRows = 42;

        ReplicaTaskResult result = new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call();

        assertEquals("42", result.watermarkCandidate());
    }

    @Test
    void succeedsWithNullWatermarkCandidateWhenSourceHasNone() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager();
        RecordingManager sink = new RecordingManager();
        sink.processedRows = 42;

        ReplicaTaskResult result = new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call();

        assertNull(result.watermarkCandidate());
    }

    @Test
    void failsTaskWhenResolveWatermarkCandidateThrows() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager();
        source.watermarkCandidateFailure = new SQLException("probe failed");
        RecordingManager sink = new RecordingManager();
        sink.processedRows = 42;

        ReplicaTask task = new ReplicaTask(0, options, new StubManagerFactory(source, sink));

        assertThrows(SQLException.class, task::call);
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy"
        });
    }

    private static final class StubManagerFactory extends ManagerFactory {
        private final ConnManager source;
        private final ConnManager sink;

        private StubManagerFactory(ConnManager source, ConnManager sink) {
            this.source = source;
            this.sink = sink;
        }

        @Override
        public ConnManager accept(ToolOptions options, DataSourceType dataSourceType) {
            return DataSourceType.SOURCE.equals(dataSourceType) ? source : sink;
        }
    }

    private static final class RecordingManager extends ConnManager {
        private int processedRows;
        private String watermarkCandidate;
        private SQLException watermarkCandidateFailure;

        @Override
        public ResultSet readTable(String tableName, String[] columns, int nThread) {
            return null;
        }

        @Override
        public int insertDataToTable(ResultSet resultSet, int taskId) {
            return processedRows;
        }

        @Override
        public String resolveWatermarkCandidate(int taskId) throws SQLException {
            if (watermarkCandidateFailure != null) {
                throw watermarkCandidateFailure;
            }
            return watermarkCandidate;
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
