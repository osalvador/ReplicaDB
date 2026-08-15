package org.replicadb.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.replicadb.cli.ToolOptions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for SqlManager.resolveWatermarkCandidate(), using PostgresqlManager as a
 * concrete implementation. Mirrors SqlManagerStagingTableTest's mocked-connection style.
 */
class SqlManagerWatermarkCandidateTest {

    private Connection mockConnection;
    private PreparedStatement mockReadStatement;
    private PreparedStatement mockProbeStatement;
    private ResultSet mockReadResultSet;
    private ResultSet mockProbeResultSet;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = mock(Connection.class);
        mockReadStatement = mock(PreparedStatement.class);
        mockProbeStatement = mock(PreparedStatement.class);
        mockReadResultSet = mock(ResultSet.class);
        mockProbeResultSet = mock(ResultSet.class);

        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
                .thenReturn(mockReadStatement, mockProbeStatement);
        when(mockReadStatement.executeQuery()).thenReturn(mockReadResultSet);
        when(mockProbeStatement.executeQuery()).thenReturn(mockProbeResultSet);

        ResultSetMetaData mockMetaData = mock(ResultSetMetaData.class);
        when(mockMetaData.getColumnCount()).thenReturn(0);
        when(mockReadResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockProbeResultSet.getMetaData()).thenReturn(mockMetaData);
    }

    private PostgresqlManager createSourceManager(String[] args) throws Exception {
        ToolOptions options = new ToolOptions(args);
        return new PostgresqlManager(options, DataSourceType.SOURCE) {
            @Override
            public Connection getConnection() {
                return mockConnection;
            }
        };
    }

    private static String[] baseArgs(String watermarkColumn) {
        if (watermarkColumn == null) {
            return new String[]{
                    "--source-connect", "jdbc:postgresql://localhost:5432/test",
                    "--source-table", "t_source",
                    "--sink-connect", "jdbc:postgresql://localhost:5432/test",
                    "--sink-table", "sink_table"
            };
        }
        return new String[]{
                "--source-connect", "jdbc:postgresql://localhost:5432/test",
                "--source-table", "t_source",
                "--sink-connect", "jdbc:postgresql://localhost:5432/test",
                "--sink-table", "sink_table",
                "--mode", "incremental",
                "--incremental-watermark-column", "c_integer"
        };
    }

    @Test
    void issuesMaxProbeWrappingLastReadSqlAndBindArgs() throws Exception {
        PostgresqlManager manager = createSourceManager(baseArgs("c_integer"));
        manager.execute("SELECT * FROM t_source WHERE c_integer > ?", (Object) 5);
        when(mockProbeResultSet.next()).thenReturn(true);
        when(mockProbeResultSet.getString(1)).thenReturn("42");

        String candidate = manager.resolveWatermarkCandidate(0);

        assertEquals("42", candidate);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        org.mockito.Mockito.verify(mockConnection, org.mockito.Mockito.times(2))
                .prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        String probeSql = sqlCaptor.getAllValues().get(1);
        assertEquals("SELECT MAX(c_integer) FROM (SELECT * FROM t_source WHERE c_integer > ?) wm_probe", probeSql);

        ArgumentCaptor<Object> bindCaptor = ArgumentCaptor.forClass(Object.class);
        org.mockito.Mockito.verify(mockProbeStatement).setObject(org.mockito.Mockito.eq(1), bindCaptor.capture());
        assertEquals(5, bindCaptor.getValue());
    }

    @Test
    void returnsNullWhenNoWatermarkColumnConfigured() throws Exception {
        PostgresqlManager manager = createSourceManager(baseArgs(null));
        manager.execute("SELECT * FROM t_source");

        assertNull(manager.resolveWatermarkCandidate(0));
    }

    @Test
    void returnsNullWhenMaxProbeIsNull() throws Exception {
        PostgresqlManager manager = createSourceManager(baseArgs("c_integer"));
        manager.execute("SELECT * FROM t_source WHERE c_integer > ?", (Object) 5);
        when(mockProbeResultSet.next()).thenReturn(true);
        when(mockProbeResultSet.getString(1)).thenReturn(null);

        assertNull(manager.resolveWatermarkCandidate(0));
    }

    @Test
    void sinkManagerNeverSetsLastReadSql() throws Exception {
        ToolOptions options = new ToolOptions(baseArgs("c_integer"));
        PostgresqlManager sinkManager = new PostgresqlManager(options, DataSourceType.SINK) {
            @Override
            public Connection getConnection() {
                return mockConnection;
            }
        };
        sinkManager.execute("INSERT INTO sink_table VALUES (?)", (Object) 1);

        assertNull(sinkManager.resolveWatermarkCandidate(0));
    }

    @Test
    void propagatesSqlExceptionFromProbeExecuteQuery() throws Exception {
        PostgresqlManager manager = createSourceManager(baseArgs("c_integer"));
        manager.execute("SELECT * FROM t_source WHERE c_integer > ?", (Object) 5);
        when(mockProbeStatement.executeQuery()).thenThrow(new SQLException("probe failed"));

        assertThrows(SQLException.class, () -> manager.resolveWatermarkCandidate(0));
    }
}
