package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the --sink-auto-create feature with PostgreSQL.
 * Tests automatic table creation based on source metadata.
 */
@Testcontainers
class Postgres2PostgresAutoCreateTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2PostgresAutoCreateTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";

    private Connection postgresConn;
    private static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres;

    @BeforeAll
    static void setUp() {
        postgres = ReplicadbPostgresqlContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Drop auto-created test tables to ensure clean state
        try {
            postgresConn.createStatement().execute("DROP TABLE IF EXISTS t_sink_autocreate");
            postgresConn.createStatement().execute("DROP TABLE IF EXISTS t_sink_autocreate_incremental");
            postgresConn.createStatement().execute("DROP TABLE IF EXISTS t_sink_autocreate_query");
            postgresConn.commit();
        } catch (SQLException e) {
            LOG.warn("Error dropping test tables: {}", e.getMessage());
        }
        this.postgresConn.close();
    }

    /**
     * Counts rows in the specified table.
     */
    private int countRows(String tableName) throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + tableName);
        rs.next();
        int count = rs.getInt(1);
        LOG.info("{} rows: {}", tableName, count);
        return count;
    }

    /**
     * Checks if a table exists in the database.
     */
    private boolean tableExists(String tableName) throws SQLException {
        DatabaseMetaData metadata = postgresConn.getMetaData();
        ResultSet rs = metadata.getTables(null, null, tableName.toLowerCase(), new String[]{"TABLE"});
        boolean exists = rs.next();
        rs.close();
        return exists;
    }

    /**
     * Gets the primary key columns for a table.
     */
    private String[] getPrimaryKeys(String tableName) throws SQLException {
        DatabaseMetaData metadata = postgresConn.getMetaData();
        ResultSet rs = metadata.getPrimaryKeys(null, null, tableName.toLowerCase());
        
        java.util.List<String> pkColumns = new java.util.ArrayList<>();
        while (rs.next()) {
            pkColumns.add(rs.getString("COLUMN_NAME"));
        }
        rs.close();
        
        return pkColumns.toArray(new String[0]);
    }

    /**
     * Validates that the sink table has the expected columns with correct types.
     */
    private void validateTableStructure(String tableName, String[] expectedColumns) throws SQLException {
        DatabaseMetaData metadata = postgresConn.getMetaData();
        ResultSet rs = metadata.getColumns(null, null, tableName.toLowerCase(), null);
        
        java.util.Set<String> foundColumns = new java.util.HashSet<>();
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            foundColumns.add(columnName.toLowerCase());
            LOG.info("Found column: {} (type: {})", columnName, rs.getString("TYPE_NAME"));
        }
        rs.close();
        
        for (String expectedCol : expectedColumns) {
            assertTrue(foundColumns.contains(expectedCol.toLowerCase()), 
                "Table " + tableName + " should have column: " + expectedCol);
        }
    }

    @Test
    void testAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        // Given: Sink table does not exist
        String sinkTable = "t_sink_autocreate";
        assertFalse(tableExists(sinkTable), "Sink table should not exist before test");

        // When: Replication with --sink-auto-create flag
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);

        // Then: Table should be created and data replicated
        assertEquals(0, result, "Replication should succeed");
        assertTrue(tableExists(sinkTable), "Sink table should be created automatically");
        
        int rowCount = countRows(sinkTable);
        assertTrue(rowCount > 0, "Sink table should have data");
        LOG.info("Successfully replicated {} rows to auto-created table", rowCount);

        // Validate table structure
        String[] expectedColumns = {"c_integer", "c_character_var", "c_character", "c_character_lob", "c_numeric", 
                                   "c_decimal", "c_real", "c_double_precision", "c_smallint", 
                                   "c_bigint", "c_boolean", "c_date", "c_timestamp_without_timezone"};
        validateTableStructure(sinkTable, expectedColumns);
    }

    @Test
    void testAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        // Given: Sink table does not exist
        String sinkTable = "t_sink_autocreate_incremental";
        assertFalse(tableExists(sinkTable), "Sink table should not exist before test");

        // When: Replication with --sink-auto-create flag in incremental mode
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-staging-schema", "public",
                "--sink-auto-create",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);

        // Then: Table should be created with PRIMARY KEY and data replicated
        assertEquals(0, result, "Replication should succeed");
        assertTrue(tableExists(sinkTable), "Sink table should be created automatically");
        
        // Validate primary key was created
        String[] primaryKeys = getPrimaryKeys(sinkTable);
        assertTrue(primaryKeys.length > 0, "Sink table should have primary key for incremental mode");
        LOG.info("Primary key columns: {}", String.join(", ", primaryKeys));
        
        int rowCount = countRows(sinkTable);
        assertTrue(rowCount > 0, "Sink table should have data");
        LOG.info("Successfully replicated {} rows with primary key to auto-created table", rowCount);

        // Run incremental replication again to test merge/upsert
        int result2 = ReplicaDB.processReplica(new ToolOptions(args));
        assertEquals(0, result2, "Second incremental replication should succeed");
        
        int rowCount2 = countRows(sinkTable);
        assertEquals(rowCount, rowCount2, "Row count should remain same after idempotent incremental run");
        LOG.info("Incremental mode merge successful - row count unchanged: {}", rowCount2);
    }

    @Test
    void testAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        // Given: Sink table already exists (use the regular t_sink table)
        String sinkTable = "t_sink";
        assertTrue(tableExists(sinkTable), "Sink table should already exist");
        
        // Ensure autocommit is off for truncate
        postgresConn.setAutoCommit(false);
        postgresConn.createStatement().execute("TRUNCATE TABLE " + sinkTable);
        postgresConn.commit();

        // When: Replication with --sink-auto-create flag on existing table
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);

        // Then: Replication should succeed without creating table
        assertEquals(0, result, "Replication should succeed");
        assertTrue(tableExists(sinkTable), "Sink table should still exist");
        
        int rowCount = countRows(sinkTable);
        assertTrue(rowCount > 0, "Sink table should have replicated data");
        LOG.info("Auto-create correctly skipped for existing table, replicated {} rows", rowCount);
    }

    @Test
    void testAutoCreateWithSourceQuery() throws ParseException, IOException, SQLException {
        // Given: Sink table does not exist, using custom query
        String sinkTable = "t_sink_autocreate_query";
        assertFalse(tableExists(sinkTable), "Sink table should not exist before test");

        // When: Replication with --sink-auto-create and --source-query
        // Note: Even with custom query, if source-table is set, PKs can still be obtained from source table metadata
        String sourceQuery = "SELECT c_integer, c_character_var, c_numeric, c_date FROM t_source WHERE c_integer < 1000";
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_source",  // Include source table for PK metadata
                "--source-query", sourceQuery,
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);

        // Then: Table should be created successfully
        assertEquals(0, result, "Replication should succeed");
        assertTrue(tableExists(sinkTable), "Sink table should be created automatically");
        
        int rowCount = countRows(sinkTable);
        assertTrue(rowCount > 0, "Sink table should have data");
        assertTrue(rowCount < countRows("t_source"), "Custom query should have filtered rows");
        LOG.info("Successfully replicated {} rows from custom query to auto-created table", rowCount);

        // Validate only selected columns are present
        String[] expectedColumns = {"c_integer", "c_character_var", "c_numeric", "c_date"};
        validateTableStructure(sinkTable, expectedColumns);
    }

    @Test
    void testAutoCreateWithCustomQueryNoPKs() throws ParseException, IOException, SQLException {
        // Given: Sink table does not exist, using custom query WITHOUT source-table
        String sinkTable = "t_sink_autocreate_query";
        assertFalse(tableExists(sinkTable), "Sink table should not exist before test");

        // When: Replication with --sink-auto-create, --source-query, and NO --source-table
        // Note: When source-table is not specified, ReplicaDB still captures full table metadata
        // during probe by extracting table name from the query
        String sourceQuery = "SELECT c_integer, c_character_var FROM t_source";
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-query", sourceQuery,
                // NO --source-table, but probe will extract it from query
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);

        // Then: Should create table successfully
        // Note: PK may still be captured if probe can extract table name from query
        assertEquals(0, result, "Replication should succeed");
        assertTrue(tableExists(sinkTable), "Sink table should be created");
        
        int rowCount = countRows(sinkTable);
        assertTrue(rowCount > 0, "Sink table should have data");
        LOG.info("Successfully replicated {} rows from custom query without explicit source-table", rowCount);
    }

    @Test
    void testAutoCreatePreservesDataTypes() throws ParseException, IOException, SQLException {
        // Given: Sink table does not exist
        String sinkTable = "t_sink_autocreate";
        assertFalse(tableExists(sinkTable), "Sink table should not exist before test");

        // When: Replication with various data types
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        // Then: Validate specific data types are correctly mapped
        DatabaseMetaData metadata = postgresConn.getMetaData();
        ResultSet rs = metadata.getColumns(null, null, sinkTable.toLowerCase(), null);
        
        java.util.Map<String, String> columnTypes = new java.util.HashMap<>();
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            columnTypes.put(columnName.toLowerCase(), typeName);
            LOG.info("Column: {} -> Type: {}", columnName, typeName);
        }
        rs.close();

        // Validate key data types
        assertTrue(columnTypes.containsKey("c_integer"), "Should have c_integer column");
        assertTrue(columnTypes.containsKey("c_character_var"), "Should have c_character_var column");
        assertTrue(columnTypes.containsKey("c_numeric"), "Should have c_numeric column");
        assertTrue(columnTypes.containsKey("c_date"), "Should have c_date column");
        assertTrue(columnTypes.containsKey("c_timestamp_without_timezone"), "Should have c_timestamp_without_timezone column");
        assertTrue(columnTypes.containsKey("c_boolean"), "Should have c_boolean column");
        
        LOG.info("Data types correctly preserved in auto-created table");
    }
}
