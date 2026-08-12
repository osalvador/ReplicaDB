package org.replicadb.manager.db2;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Db2ManagerTest {

    @Test
    void resolvePartitionAliasWithoutCollisionUsesBaseAlias() throws Exception {
        Db2Manager manager = createManager(mock(Connection.class), false);

        String alias = invokeResolvePartitionAlias(manager, Set.of("ID", "NAME"));

        assertEquals("REPLICADB_PARTITION_RN", alias);
    }

    @Test
    void resolvePartitionAliasCollisionUsesFirstAvailableSuffix() throws Exception {
        Db2Manager manager = createManager(mock(Connection.class), false);
        Set<String> sourceLabels = Set.of(
                "ID",
                "REPLICADB_PARTITION_RN",
                "REPLICADB_PARTITION_RN_1",
                "REPLICADB_PARTITION_RN_3"
        );

        String alias = invokeResolvePartitionAlias(manager, sourceLabels);

        assertEquals("REPLICADB_PARTITION_RN_2", alias);
    }

    @Test
    void resolvePartitionProjectionWithExplicitColumnsDoesNotProbeConnection() throws Exception {
        Connection connection = mock(Connection.class);
        Db2Manager manager = createManager(connection, false);

        Object projection = invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "id, name");

        assertEquals("id, name", projectionColumns(projection));
        assertEquals("REPLICADB_PARTITION_RN", projectionAlias(projection));
        verify(connection, org.mockito.Mockito.never()).prepareStatement(anyString());
    }

    @Test
    void resolvePartitionProjectionWildcardUsesLabelsAndClosesResources() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.prepareStatement("SELECT * FROM (SELECT * FROM source_table) PROBE WHERE 1=0"))
                .thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnLabel(1)).thenReturn("ID");
        when(metadata.getColumnLabel(2)).thenReturn("NAME");
        Db2Manager manager = createManager(connection, false);

        Object projection = invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "*");

        assertEquals("ID,NAME", projectionColumns(projection));
        assertEquals("REPLICADB_PARTITION_RN", projectionAlias(projection));
        verify(statement).close();
        verify(resultSet).close();
    }

    @Test
    void resolvePartitionProjectionWildcardFallsBackToColumnNameAndEscapesQuotedIdentifiers() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnLabel(1)).thenReturn("A\"B");
        when(metadata.getColumnLabel(2)).thenReturn("");
        when(metadata.getColumnName(2)).thenReturn("SECOND");
        Db2Manager manager = createManager(connection, true);

        Object projection = invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "*");

        assertEquals("\"A\"\"B\",\"SECOND\"", projectionColumns(projection));
        assertEquals("REPLICADB_PARTITION_RN", projectionAlias(projection));
    }

    @Test
    void resolvePartitionProjectionWildcardRejectsDuplicateLabelsCaseInsensitively() throws Exception {
        ResultSetMetaData metadata = metadataWithLabels("ID", "id");
        Db2Manager manager = createManager(connectionWithMetadata(metadata), false);

        SQLException exception = assertThrows(SQLException.class,
                () -> invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "*"));

        assertTrue(exception.getMessage().contains("Unable to resolve DB2 source columns for parallel read"));
        assertTrue(exception.getMessage().contains("duplicate column label"));
    }

    @Test
    void resolvePartitionProjectionWildcardRejectsEmptyMetadata() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(0);
        Db2Manager manager = createManager(connectionWithMetadata(metadata), false);

        SQLException exception = assertThrows(SQLException.class,
                () -> invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "*"));

        assertTrue(exception.getMessage().contains("metadata is empty"));
    }

    @Test
    void resolvePartitionProjectionWildcardRejectsUnnamedColumn() throws Exception {
        ResultSetMetaData metadata = metadataWithLabelsAndNames(null, null);
        Db2Manager manager = createManager(connectionWithMetadata(metadata), false);

        SQLException exception = assertThrows(SQLException.class,
                () -> invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "*"));

        assertTrue(exception.getMessage().contains("column metadata is unnamed"));
    }

    @Test
    void resolvePartitionProjectionWildcardWrapsProbeFailureWithoutExposingSql() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("driver failure"));
        Db2Manager manager = createManager(connection, false);

        SQLException exception = assertThrows(SQLException.class,
                () -> invokeResolvePartitionProjection(manager, "SELECT * FROM source_table", "*"));

        assertEquals("Unable to resolve DB2 source columns for parallel read", exception.getMessage());
        assertTrue(exception.getCause() instanceof SQLException);
        assertFalse(exception.getMessage().contains("source_table"));
    }

    private Db2Manager createManager(Connection mockConnection, boolean quotedIdentifiers) throws Exception {
        String[] args = {
                "--source-connect", "jdbc:db2://localhost:50000/testdb",
                "--source-table", "source_table",
                "--sink-connect", "jdbc:postgresql://localhost:5432/testdb",
                "--sink-table", "sink_table"
        };
        if (quotedIdentifiers) {
            String[] quotedArgs = new String[args.length + 1];
            System.arraycopy(args, 0, quotedArgs, 0, args.length);
            quotedArgs[args.length] = "--quoted-identifiers";
            args = quotedArgs;
        }
        ToolOptions options = new ToolOptions(args);
        return new Db2Manager(options, DataSourceType.SOURCE) {
            @Override
            public Connection getConnection() {
                return mockConnection;
            }
        };
    }

    private Connection connectionWithMetadata(ResultSetMetaData metadata) throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metadata);
        return connection;
    }

    private ResultSetMetaData metadataWithLabels(String... labels) throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(labels.length);
        for (int i = 0; i < labels.length; i++) {
            when(metadata.getColumnLabel(i + 1)).thenReturn(labels[i]);
        }
        return metadata;
    }

    private ResultSetMetaData metadataWithLabelsAndNames(String label, String name) throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnLabel(1)).thenReturn(label);
        when(metadata.getColumnName(1)).thenReturn(name);
        return metadata;
    }

    private String invokeResolvePartitionAlias(Db2Manager manager, Set<String> sourceLabels) throws Exception {
        Method method = Db2Manager.class.getDeclaredMethod("resolvePartitionAlias", Set.class);
        method.setAccessible(true);
        return (String) method.invoke(manager, new HashSet<>(sourceLabels));
    }

    private Object invokeResolvePartitionProjection(Db2Manager manager, String baseQuery, String sourceColumns)
            throws Exception {
        Method method = Db2Manager.class.getDeclaredMethod(
                "resolvePartitionProjection", String.class, String.class);
        method.setAccessible(true);
        try {
            return method.invoke(manager, baseQuery, sourceColumns);
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw exception;
        }
    }

    private String projectionColumns(Object projection) throws Exception {
        return projectionField(projection, "columns");
    }

    private String projectionAlias(Object projection) throws Exception {
        return projectionField(projection, "partitionAlias");
    }

    private String projectionField(Object projection, String fieldName) throws Exception {
        var field = projection.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (String) field.get(projection);
    }
}
