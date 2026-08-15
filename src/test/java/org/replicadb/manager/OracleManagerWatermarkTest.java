package org.replicadb.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.ColumnDescriptor;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for watermark predicate injection in OracleManager's source-where and
 * full-table readTable() branches. The partition predicate (ora_hash/dummy 0=?) is always
 * the last bind; the watermark bind, when present, comes first.
 * Mirrors SqlManagerStagingTableTest's mocked-connection style.
 */
class OracleManagerWatermarkTest {

    private Connection mockConnection;
    private PreparedStatement mockStatement;

    @BeforeEach
    void setUp() throws Exception {
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        ResultSetMetaData mockMetaData = mock(ResultSetMetaData.class);
        when(mockMetaData.getColumnCount()).thenReturn(0);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(mockResultSet);

        Statement mockSessionStatement = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(mockSessionStatement);

        DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
        when(mockDatabaseMetaData.getDatabaseMajorVersion()).thenReturn(19);
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
    }

    private OracleManager createManager(String[] args) throws Exception {
        ToolOptions options = new ToolOptions(args);
        options.setSourceColumnDescriptors(List.of(new ColumnDescriptor("c_integer", Types.INTEGER, 10, 0, 1)));
        return new OracleManager(options, DataSourceType.SOURCE) {
            @Override
            public Connection getConnection() {
                return mockConnection;
            }
        };
    }

    private static String[] baseArgs(String[] extra) {
        String[] base = {
                "--source-connect", "jdbc:oracle:thin:@localhost:1521:test",
                "--source-table", "t_source",
                "--sink-connect", "jdbc:oracle:thin:@localhost:1521:test",
                "--sink-table", "sink_table",
                "--jobs", "1"
        };
        if (extra.length == 0) {
            return base;
        }
        String[] all = new String[base.length + extra.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extra, 0, all, base.length, extra.length);
        return all;
    }

    @Test
    void injectsPredicateBeforePartitionPredicateInFullTableBranch() throws Exception {
        OracleManager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "5"
        }));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("c_integer > ?"));
        assertTrue(sql.indexOf("c_integer > ?") < sql.indexOf("0 = ?"));

        ArgumentCaptor<Object> bindCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mockStatement).setObject(org.mockito.Mockito.eq(1), bindCaptor.capture());
        assertEquals(new java.math.BigDecimal("5"), bindCaptor.getValue());
        verify(mockStatement).setObject(org.mockito.Mockito.eq(2), org.mockito.Mockito.eq(0));
    }

    @Test
    void injectsPredicateBeforePartitionPredicateInSourceWhereBranch() throws Exception {
        OracleManager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "5", "--source-where", "1=1"
        }));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("c_integer > ?"));
        assertTrue(sql.indexOf("c_integer > ?") < sql.indexOf("0 = ?"));
    }

    @Test
    void skipsPredicateWhenColumnConfiguredWithoutValue() throws Exception {
        OracleManager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer"
        }));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        assertFalse(sqlCaptor.getValue().contains("c_integer > ?"));
        verify(mockStatement, never()).setObject(org.mockito.Mockito.eq(2), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void sourceQueryBranchUnchangedWhenWatermarkConfigured() throws Exception {
        OracleManager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "5", "--source-query", "SELECT * FROM t_source"
        }));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        assertFalse(sqlCaptor.getValue().contains("c_integer > ?"));
        verify(mockStatement, never()).setObject(org.mockito.Mockito.eq(2), org.mockito.ArgumentMatchers.any());
    }
}
