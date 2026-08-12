package org.replicadb;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.AzureAuthenticationMode;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaTaskAuthenticationFailureTest {

    @Test
    void closesBothManagersWhenSourceAuthenticationFails() throws Exception {
        ToolOptions options = options();
        RuntimeException authenticationFailure = new RuntimeException("source authentication failed");
        RecordingManager source = new RecordingManager(authenticationFailure, null);
        RecordingManager sink = new RecordingManager(null, null);

        Exception thrown = assertThrows(Exception.class,
                () -> new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call());

        assertSame(authenticationFailure, thrown);
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void closesBothManagersWhenSinkAuthenticationFails() throws Exception {
        ToolOptions options = options();
        RuntimeException authenticationFailure = new RuntimeException("sink authentication failed");
        RecordingManager source = new RecordingManager(null, null);
        RecordingManager sink = new RecordingManager(authenticationFailure, null);

        Exception thrown = assertThrows(Exception.class,
                () -> new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call());

        assertSame(authenticationFailure, thrown);
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void suppressesCloseFailureWithoutReplacingAuthenticationFailure() throws Exception {
        ToolOptions options = options();
        RuntimeException authenticationFailure = new RuntimeException("sink authentication failed");
        SQLException closeFailure = new SQLException("source close failed");
        RecordingManager source = new RecordingManager(null, closeFailure);
        RecordingManager sink = new RecordingManager(authenticationFailure, null);

        Exception thrown = assertThrows(Exception.class,
                () -> new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call());

        assertSame(authenticationFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void rejectsInteractiveAuthenticationWithParallelJobs() throws Exception {
        ToolOptions options = options();
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new ManagerFactory().validateAzureAuthenticationConfiguration(options));

        assertTrue(exception.getMessage().contains("jobs=1"));
    }

    @Test
    void rejectsRawInteractiveAuthenticationWithParallelJobs() throws Exception {
        ToolOptions options = options();
        options.setJobs("2");
        options.setSourceConnect("jdbc:sqlserver://source;authentication=ActiveDirectoryInteractive");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new ManagerFactory().validateAzureAuthenticationConfiguration(options));

        assertTrue(exception.getMessage().contains("jobs=1"));
    }

    @Test
    void rejectsFirstClassAuthenticationOnNonSqlServerConnection() throws Exception {
        ToolOptions options = options();
        options.setSourceConnect("jdbc:postgresql://localhost/source");
        options.setJobs("1");
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new ManagerFactory().validateAzureAuthenticationConfiguration(options));

        assertTrue(exception.getMessage().contains("SQL Server"));
    }

    @Test
    void processReplicaReturnsErrorBeforeSentryForInvalidAzureConfiguration() throws Exception {
        ToolOptions options = options();
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE);

        assertEquals(1, ReplicaDB.processReplica(options));
    }

    private ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlserver://source",
                "--sink-connect", "jdbc:sqlserver://sink"
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
        private final Exception connectionFailure;
        private final Exception closeFailure;
        private boolean closed;

        private RecordingManager(Exception connectionFailure, Exception closeFailure) {
            this.connectionFailure = connectionFailure;
            this.closeFailure = closeFailure;
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
        public Connection getConnection() throws Exception {
            if (connectionFailure != null) {
                throw connectionFailure;
            }
            return null;
        }

        @Override
        public String getDriverClass() {
            return "";
        }

        @Override
        public void close() throws SQLException {
            closed = true;
            if (closeFailure != null) {
                throw (SQLException) closeFailure;
            }
        }

        @Override
        public void cleanUp() {
        }

        @Override
        public void release() {
        }

        @Override
        public Future<Integer> preSinkTasks(ExecutorService executor) {
            return null;
        }

        @Override
        public void preSourceTasks() {
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
