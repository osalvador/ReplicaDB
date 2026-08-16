package org.replicadb;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaDBRunCountersTest {

    @Test
    void successfulRunStoresRowsAndDuration() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        RecordingManager sink = new RecordingManager(options);
        sink.rowsProcessed = 7;

        assertEquals(0, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertEquals(7, options.getExecutionContext().getRowsProcessed());
        assertTrue(options.getExecutionContext().getDurationMillis() >= 0);
    }

    @Test
    void mergeFailurePreservesRowsAndDuration() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        RecordingManager sink = new RecordingManager(options);
        sink.rowsProcessed = 7;
        sink.failPostSinkTasks = true;

        assertEquals(1, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertEquals(7, options.getExecutionContext().getRowsProcessed());
        assertTrue(options.getExecutionContext().getDurationMillis() >= 0);
    }

    @Test
    void explicitCancellationPreservesRowsAndDuration() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        RecordingManager sink = new RecordingManager(options);
        sink.rowsProcessed = 7;
        sink.throwCancelledOnPostSinkTasks = true;

        assertEquals(ReplicaDB.CANCELLED, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertEquals(7, options.getExecutionContext().getRowsProcessed());
        assertTrue(options.getExecutionContext().getDurationMillis() >= 0);
    }

    @Test
    void flagBasedCancellationPreservesRowsAndDuration() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        RecordingManager sink = new RecordingManager(options);
        sink.rowsProcessed = 7;
        sink.requestCancellationBeforePostSinkTasks = true;
        sink.throwPlainSqlExceptionOnPostSinkTasks = true;

        assertEquals(ReplicaDB.CANCELLED, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertEquals(7, options.getExecutionContext().getRowsProcessed());
        assertTrue(options.getExecutionContext().getDurationMillis() >= 0);
    }

    @Test
    void validationFailureLeavesCountersAtDefaults() throws Exception {
        ToolOptions options = options();

        assertEquals(1, ReplicaDB.processReplica(options, new FailingValidationManagerFactory()));

        assertEquals(0, options.getExecutionContext().getRowsProcessed());
        assertEquals(0, options.getExecutionContext().getDurationMillis());
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy",
                "--mode", "complete",
                "--jobs", "1"
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
        public void validateAzureAuthenticationConfiguration(ToolOptions options) {
        }

        @Override
        public ConnManager accept(ToolOptions options, DataSourceType dataSourceType) {
            return DataSourceType.SOURCE.equals(dataSourceType) ? source : sink;
        }
    }

    private static final class FailingValidationManagerFactory extends ManagerFactory {
        @Override
        public void validateAzureAuthenticationConfiguration(ToolOptions options) {
            throw new IllegalArgumentException("invalid authentication");
        }
    }

    private static final class RecordingManager extends ConnManager {
        private int rowsProcessed;
        private boolean failPostSinkTasks;
        private boolean throwCancelledOnPostSinkTasks;
        private boolean requestCancellationBeforePostSinkTasks;
        private boolean throwPlainSqlExceptionOnPostSinkTasks;

        private RecordingManager(ToolOptions options) {
            this.options = options;
        }

        @Override
        public ResultSet readTable(String tableName, String[] columns, int nThread) {
            return null;
        }

        @Override
        public int insertDataToTable(ResultSet resultSet, int taskId) {
            return rowsProcessed;
        }

        @Override
        public String resolveWatermarkCandidate(int taskId) {
            return null;
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
        public void postSinkTasks() throws Exception {
            if (requestCancellationBeforePostSinkTasks) {
                options.getExecutionContext().requestCancellation();
            }
            if (throwCancelledOnPostSinkTasks) {
                throw new ReplicationCancelledException("test cancellation");
            }
            if (throwPlainSqlExceptionOnPostSinkTasks) {
                throw new SQLException("driver reported statement cancellation");
            }
            if (failPostSinkTasks) {
                throw new Exception("merge failed");
            }
        }

        @Override
        public String[] getSinkPrimaryKeys(String tableName) {
            return new String[0];
        }
    }
}