package org.replicadb.db2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.db2.Db2Manager;
import org.replicadb.manager.util.ColumnDescriptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Unit tests for watermark predicate injection in Db2Manager's readTable(). DB2's partitioning
 * is entirely literal-based (MOD(ROW_NUMBER()...) inlined, not bound), so the watermark predicate
 * is the only bind placeholder either the jobs==1 or jobs&gt;1 branch will ever contain.
 * Mirrors SqlManagerStagingTableTest's mocked-connection style.
 */
class Db2ManagerWatermarkTest {

    private Connection mockConnection;
    private PreparedStatement mockStatement;

    @BeforeEach
    void setUp() throws Exception {
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        ResultSetMetaData mockMetaData = mock(ResultSetMetaData.class);
        when(mockMetaData.getColumnCount()).thenReturn(1);
        when(mockMetaData.getColumnLabel(1)).thenReturn("C_INTEGER");
        when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockStatement);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(mockResultSet);
    }

    private Db2Manager createManager(String[] args) throws Exception {
        ToolOptions options = new ToolOptions(args);
        options.setSourceColumnDescriptors(List.of(new ColumnDescriptor("c_integer", Types.INTEGER, 10, 0, 1)));
        return new Db2Manager(options, DataSourceType.SOURCE) {
            @Override
            public Connection getConnection() {
                return mockConnection;
            }
        };
    }

    private static String[] baseArgs(String[] extra, String jobs) {
        String[] base = {
                "--source-connect", "jdbc:db2://localhost:50000/test",
                "--source-table", "t_source",
                "--sink-connect", "jdbc:db2://localhost:50000/test",
                "--sink-table", "sink_table",
                "--jobs", jobs
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
    void injectsSinglePredicateBindWhenJobsIsOne() throws Exception {
        Db2Manager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "5"
        }, "1"));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("c_integer > ?"));
        assertEquals(1, countPlaceholders(sql));

        ArgumentCaptor<Object> bindCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mockStatement).setObject(org.mockito.Mockito.eq(1), bindCaptor.capture());
        assertEquals(new java.math.BigDecimal("5"), bindCaptor.getValue());
    }

    @Test
    void injectsSinglePredicateBindWhenParallel() throws Exception {
        Db2Manager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "5", "--source-columns", "c_integer"
        }, "4"));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("c_integer > ?"));
        assertTrue(sql.contains("MOD(ROW_NUMBER()"));
        assertEquals(1, countPlaceholders(sql));
    }

    @Test
    void skipsPredicateWhenColumnConfiguredWithoutValue() throws Exception {
        Db2Manager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer"
        }, "1"));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        assertFalse(sqlCaptor.getValue().contains("c_integer > ?"));
        verify(mockStatement, never()).setObject(anyInt(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void sourceQueryBranchUnchangedWhenWatermarkConfigured() throws Exception {
        Db2Manager manager = createManager(baseArgs(new String[]{
                "--mode", "incremental", "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "5", "--source-query", "SELECT * FROM t_source"
        }, "1"));

        manager.readTable(null, null, 0);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture(), anyInt(), anyInt());
        assertFalse(sqlCaptor.getValue().contains("c_integer > ?"));
        verify(mockStatement, never()).setObject(anyInt(), org.mockito.ArgumentMatchers.any());
    }

    private static int countPlaceholders(String sql) {
        Pattern pattern = Pattern.compile("\\?");
        Matcher matcher = pattern.matcher(sql);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
