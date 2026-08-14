package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqlManagerCancellationTest {

    @Test
    void cancelsTheStatementCreatedByExecute() throws Exception {
        ToolOptions options = options();
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = resultSet();
        when(connection.prepareStatement(anyString(), eq(ResultSet.TYPE_FORWARD_ONLY),
                eq(ResultSet.CONCUR_READ_ONLY))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        TestSqlManager manager = new TestSqlManager(options, connection);

        assertSame(resultSet, manager.query("SELECT 1"));
        options.getExecutionContext().requestCancellation();

        verify(statement).cancel();
    }

    @Test
    void releaseRemovesTheStatementFromCancellationTracking() throws Exception {
        ToolOptions options = options();
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = resultSet();
        when(connection.prepareStatement(anyString(), eq(ResultSet.TYPE_FORWARD_ONLY),
                eq(ResultSet.CONCUR_READ_ONLY))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        TestSqlManager manager = new TestSqlManager(options, connection);

        manager.query("SELECT 1");
        manager.release();
        options.getExecutionContext().requestCancellation();

        verify(statement).close();
        verify(statement, never()).cancel();
    }

    @Test
    void atomicInsertChecksCancellationBeforeCreatingSql() throws Exception {
        ToolOptions options = options();
        TestSqlManager manager = new TestSqlManager(options, null);
        options.getExecutionContext().requestCancellation();

        assertThrows(org.replicadb.execution.ReplicationCancelledException.class,
                manager::atomicInsertStagingTable);
    }

    private static ResultSet resultSet() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(0);
        return resultSet;
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy"
        });
    }

    private static final class TestSqlManager extends PostgresqlManager {

        private final Connection connection;

        private TestSqlManager(ToolOptions options, Connection connection) {
            super(options, DataSourceType.SOURCE);
            this.connection = connection;
        }

        private ResultSet query(String sql) throws Exception {
            return execute(sql);
        }

        @Override
        public Connection getConnection() {
            return connection;
        }
    }
}