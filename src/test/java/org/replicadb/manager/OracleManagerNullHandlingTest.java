package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test OracleManager NULL handling for primitive types.
 * Verifies that NULL values are preserved during replication (not converted to 0, 0.0, epoch dates, etc.)
 */
@Testcontainers
class OracleManagerNullHandlingTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";

    private Connection oracleConn;
    private static ReplicadbOracleContainer oracle;

    @BeforeAll
    static void setUp() {
        oracle = ReplicadbOracleContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connection
        this.oracleConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.oracleConn.close();
    }

    /**
     * Test that NULL INTEGER is preserved (not converted to 0).
     * This is the core issue from #51 - Denodo NULL INTEGER → Oracle 0.
     */
    @Test
    void testNullIntegerPreserved() throws Exception {
        // Replicate all data (includes row 4096 with NULL INTEGER)
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        // Verify NULL row exists (row 4096)
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        assertEquals(1, rs.getInt(1), "Row 4096 should exist");

        // Critical assertion: C_SMALLINT should be NULL, not 0
        rs = stmt.executeQuery("SELECT C_SMALLINT FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getInt(1); // Call getter to set wasNull flag
        assertTrue(rs.wasNull(), "C_SMALLINT in row 4096 should be NULL (not 0)");

        // Verify it's NOT stored as 0 (the bug behavior)
        rs = stmt.executeQuery("SELECT COUNT(*) FROM t_sink WHERE C_INTEGER = 4096 AND C_SMALLINT = 0");
        rs.next();
        assertEquals(0, rs.getInt(1), "C_SMALLINT should NOT be 0 (bug would convert NULL→0)");
    }

    /**
     * Test that NULL BIGINT/NUMERIC is preserved.
     */
    @Test
    void testNullBigDecimalPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        
        // Test BIGINT NULL
        ResultSet rs = stmt.executeQuery("SELECT C_BIGINT FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getBigDecimal(1);
        assertTrue(rs.wasNull(), "C_BIGINT should be NULL");

        // Test NUMERIC NULL
        rs = stmt.executeQuery("SELECT C_NUMERIC FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getBigDecimal(1);
        assertTrue(rs.wasNull(), "C_NUMERIC should be NULL");

        // Test DECIMAL NULL
        rs = stmt.executeQuery("SELECT C_DECIMAL FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getBigDecimal(1);
        assertTrue(rs.wasNull(), "C_DECIMAL should be NULL");
    }

    /**
     * Test that NULL DOUBLE is preserved (not converted to 0.0).
     */
    @Test
    void testNullDoublePreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_DOUBLE_PRECISION FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getDouble(1);
        assertTrue(rs.wasNull(), "C_DOUBLE_PRECISION should be NULL (not 0.0)");

        // Verify it's NOT stored as 0.0
        rs = stmt.executeQuery("SELECT COUNT(*) FROM t_sink WHERE C_INTEGER = 4096 AND C_DOUBLE_PRECISION = 0.0");
        rs.next();
        assertEquals(0, rs.getInt(1), "C_DOUBLE_PRECISION should NOT be 0.0");
    }

    /**
     * Test that NULL FLOAT/REAL is preserved.
     */
    @Test
    void testNullFloatPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        
        // Test REAL NULL
        ResultSet rs = stmt.executeQuery("SELECT C_REAL FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getFloat(1);
        assertTrue(rs.wasNull(), "C_REAL should be NULL (not 0.0f)");

        // Test FLOAT NULL
        rs = stmt.executeQuery("SELECT C_FLOAT FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        rs.getFloat(1);
        assertTrue(rs.wasNull(), "C_FLOAT should be NULL");
    }

    /**
     * Test that NULL DATE is preserved (not converted to epoch date).
     */
    @Test
    void testNullDatePreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_DATE FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        Date dateVal = rs.getDate(1);
        assertTrue(rs.wasNull() || dateVal == null, "C_DATE should be NULL (not epoch date)");
    }

    /**
     * Test that NULL TIMESTAMP is preserved.
     */
    @Test
    void testNullTimestampPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        
        // Test TIMESTAMP WITHOUT TIMEZONE
        ResultSet rs = stmt.executeQuery("SELECT C_TIMESTAMP_WITHOUT_TIMEZONE FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        Timestamp tsVal = rs.getTimestamp(1);
        assertTrue(rs.wasNull() || tsVal == null, "C_TIMESTAMP_WITHOUT_TIMEZONE should be NULL");

        // Test TIMESTAMP WITH TIMEZONE
        rs = stmt.executeQuery("SELECT C_TIMESTAMP_WITH_TIMEZONE FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        tsVal = rs.getTimestamp(1);
        assertTrue(rs.wasNull() || tsVal == null, "C_TIMESTAMP_WITH_TIMEZONE should be NULL");
    }

    /**
     * Test that NULL VARCHAR is preserved (not converted to empty string).
     */
    @Test
    void testNullStringPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        
        // Test CHARACTER NULL
        ResultSet rs = stmt.executeQuery("SELECT C_CHARACTER FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        String strVal = rs.getString(1);
        assertTrue(rs.wasNull() || strVal == null, "C_CHARACTER should be NULL (not empty string)");

        // Test CHARACTER_VAR NULL
        rs = stmt.executeQuery("SELECT C_CHARACTER_VAR FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        strVal = rs.getString(1);
        assertTrue(rs.wasNull() || strVal == null, "C_CHARACTER_VAR should be NULL");
    }

    /**
     * Test that NULL BINARY is preserved (not converted to empty bytes).
     */
    @Test
    void testNullBinaryPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        
        // Test BINARY NULL
        ResultSet rs = stmt.executeQuery("SELECT C_BINARY FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        byte[] bytesVal = rs.getBytes(1);
        assertTrue(rs.wasNull() || bytesVal == null, "C_BINARY should be NULL (not empty bytes)");

        // Test BINARY_VAR NULL
        rs = stmt.executeQuery("SELECT C_BINARY_VAR FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        bytesVal = rs.getBytes(1);
        assertTrue(rs.wasNull() || bytesVal == null, "C_BINARY_VAR should be NULL");
    }

    /**
     * Test that NULL BOOLEAN is preserved (Oracle stores as VARCHAR "true"/"false").
     */
    @Test
    void testNullBooleanPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_BOOLEAN FROM t_sink WHERE C_INTEGER = 4096");
        rs.next();
        String boolVal = rs.getString(1); // Oracle stores BOOLEAN as VARCHAR
        assertTrue(rs.wasNull() || boolVal == null, "C_BOOLEAN should be NULL (not 'false')");
    }
}
