package org.replicadb.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the widened metadata-probe gate in SqlManager.preSourceTasks(),
 * using PostgresqlManager as a concrete implementation. Mirrors SqlManagerStagingTableTest.
 */
class SqlManagerWatermarkMetadataProbeTest {

    private Connection mockConnection;
    private Statement mockStatement;
    private ResultSet mockResultSet;
    private ResultSetMetaData mockResultSetMetaData;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = mock(Connection.class);
        mockStatement = mock(Statement.class);
        mockResultSet = mock(ResultSet.class);
        mockResultSetMetaData = mock(ResultSetMetaData.class);

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
        when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(mockResultSetMetaData.getColumnName(1)).thenReturn("C_INTEGER");
        when(mockResultSetMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(mockResultSetMetaData.getPrecision(1)).thenReturn(10);
        when(mockResultSetMetaData.getScale(1)).thenReturn(0);
        when(mockResultSetMetaData.isNullable(1)).thenReturn(1);
    }

    private PostgresqlManager createManagerWithMockConnection(String[] args) throws Exception {
        ToolOptions options = new ToolOptions(args);
        return new PostgresqlManager(options, DataSourceType.SOURCE) {
            @Override
            public Connection getConnection() {
                return mockConnection;
            }
        };
    }

    @Test
    void probesMetadataWhenWatermarkColumnSetWithoutAutoCreate() throws Exception {
        String[] args = {
                "--source-connect", "jdbc:postgresql://localhost:5432/test",
                "--source-query", "SELECT 1",
                "--sink-connect", "jdbc:postgresql://localhost:5432/test",
                "--sink-table", "sink_table",
                "--mode", "incremental",
                "--incremental-watermark-column", "c_integer",
                "--jobs", "1"
        };

        PostgresqlManager manager = createManagerWithMockConnection(args);
        manager.preSourceTasks();

        verify(mockStatement).executeQuery(anyString());
        assertEquals(1, manager.options.getSourceColumnDescriptors().size());
        assertTrue(manager.options.getSourceColumnDescriptors().get(0).getColumnName().equalsIgnoreCase("c_integer"));
    }

    @Test
    void skipsProbeWhenNeitherAutoCreateNorWatermarkColumnSet() throws Exception {
        String[] args = {
                "--source-connect", "jdbc:postgresql://localhost:5432/test",
                "--source-query", "SELECT 1",
                "--sink-connect", "jdbc:postgresql://localhost:5432/test",
                "--sink-table", "sink_table",
                "--jobs", "1"
        };

        PostgresqlManager manager = createManagerWithMockConnection(args);
        manager.preSourceTasks();

        verify(mockStatement, never()).executeQuery(anyString());
    }

    @Test
    void throwsWhenWatermarkColumnIsAbsentFromProbedMetadata() throws Exception {
        String[] args = {
                "--source-connect", "jdbc:postgresql://localhost:5432/test",
                "--source-query", "SELECT 1",
                "--sink-connect", "jdbc:postgresql://localhost:5432/test",
                "--sink-table", "sink_table",
                "--mode", "incremental",
                "--incremental-watermark-column", "unknown_column",
                "--jobs", "1"
        };

        PostgresqlManager manager = createManagerWithMockConnection(args);

        assertThrows(IllegalArgumentException.class, manager::preSourceTasks);
    }
}
