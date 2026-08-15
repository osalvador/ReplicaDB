package org.replicadb;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;
import org.replicadb.manager.util.ColumnDescriptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies the reduced watermark candidate is exposed on ReplicationExecutionContext only after
 * a successful merge, and stays unset on merge failure or either cancellation code path.
 * Mirrors ReplicaDBCancellationTest's stub-manager style.
 */
class ReplicaDBWatermarkCommitTest {

    @Test
    void successfulRunSetsReducedWatermarkCandidateOnContext() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        source.watermarkCandidate = "42";
        RecordingManager sink = new RecordingManager(options);

        assertEquals(0, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertEquals("42", options.getExecutionContext().getWatermarkCandidate());
    }

    @Test
    void mergeFailureLeavesWatermarkCandidateNull() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        source.watermarkCandidate = "42";
        RecordingManager sink = new RecordingManager(options);
        sink.failPostSinkTasks = true;

        assertEquals(1, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertNull(options.getExecutionContext().getWatermarkCandidate());
    }

    @Test
    void explicitCancellationLeavesWatermarkCandidateNull() throws Exception {
        ToolOptions options = options();
        RecordingManager source = new RecordingManager(options);
        source.watermarkCandidate = "42";
        source.throwCancelledOnResolve = true;
        RecordingManager sink = new RecordingManager(options);

        assertEquals(ReplicaDB.CANCELLED, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertNull(options.getExecutionContext().getWatermarkCandidate());
    }

    @Test
    void flagBasedCancellationLeavesWatermarkCandidateNull() throws Exception {
        ToolOptions options = options();
        options.getExecutionContext().requestCancellation();
        RecordingManager source = new RecordingManager(options);
        source.watermarkCandidate = "42";
        source.throwPlainSqlExceptionOnResolve = true;
        RecordingManager sink = new RecordingManager(options);

        assertEquals(ReplicaDB.CANCELLED, ReplicaDB.processReplica(options, new StubManagerFactory(source, sink)));

        assertNull(options.getExecutionContext().getWatermarkCandidate());
    }

    private static ToolOptions options() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy",
                "--mode", "incremental",
                "--incremental-watermark-column", "c_integer",
                "--jobs", "1"
        });
        options.setSourceColumnDescriptors(List.of(new ColumnDescriptor("c_integer", Types.INTEGER, 10, 0, 1)));
        return options;
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

    private static final class RecordingManager extends ConnManager {
        private volatile String watermarkCandidate;
        private volatile boolean throwCancelledOnResolve;
        private volatile boolean throwPlainSqlExceptionOnResolve;
        private volatile boolean failPostSinkTasks;

        private RecordingManager(ToolOptions options) {
            this.options = options;
        }

        @Override
        public ResultSet readTable(String tableName, String[] columns, int nThread) {
            return null;
        }

        @Override
        public int insertDataToTable(ResultSet resultSet, int taskId) {
            return 1;
        }

        @Override
        public String resolveWatermarkCandidate(int taskId) throws SQLException {
            if (throwCancelledOnResolve) {
                throw new ReplicationCancelledException("test cancellation");
            }
            if (throwPlainSqlExceptionOnResolve) {
                throw new SQLException("driver reported statement cancellation");
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
        public void postSinkTasks() throws Exception {
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
