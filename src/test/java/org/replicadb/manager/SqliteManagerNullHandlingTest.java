package org.replicadb.manager;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.utils.ScriptRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that NULL values are preserved during SQLite-to-SQLite replication.
 * Uses two separate database files (source.db / sink.db) to avoid SQLite single-writer locking.
 * Regression guard for issue #276.
 */
class SqliteManagerNullHandlingTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();

    @TempDir
    static Path tempDir;

    private static String sourceUrl;
    private static String sinkUrl;

    private Connection sinkConn;

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        sourceUrl = "jdbc:sqlite:" + tempDir.resolve("source.db");
        sinkUrl   = "jdbc:sqlite:" + tempDir.resolve("sink.db");
        try (Connection conn = DriverManager.getConnection(sourceUrl)) {
            new ScriptRunner(conn, false, true)
                .runScript(new BufferedReader(new FileReader(RESOURCE_DIR + "/sqlite/sqlite-source.sql")));
        }
        try (Connection conn = DriverManager.getConnection(sinkUrl)) {
            new ScriptRunner(conn, false, true)
                .runScript(new BufferedReader(new FileReader(RESOURCE_DIR + "/sinks/sqlite-sink.sql")));
        }
    }

    @BeforeEach
    void before() throws SQLException {
        this.sinkConn = DriverManager.getConnection(sinkUrl);
    }

    @AfterEach
    void tearDown() throws SQLException {
        sinkConn.createStatement().execute("DELETE FROM t_sink");
        sinkConn.close();
    }

    /**
     * Test that NULL INTEGER is preserved (not converted to 0).
     * SQLite has a NULL test row already in sqlite-source.sql.
     */
    @Test
    void testNullIntegerPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_SMALLINT FROM t_sink WHERE C_SMALLINT IS NULL");
        assertTrue(rs.next(), "Should find at least one row with NULL C_SMALLINT");
        rs.getInt(1);
        assertTrue(rs.wasNull(), "C_SMALLINT should be NULL (not 0)");
    }

    /**
     * Test that NULL BIGINT/NUMERIC is preserved.
     */
    @Test
    void testNullBigDecimalPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        
        // Test BIGINT NULL
        ResultSet rs = stmt.executeQuery("SELECT C_BIGINT FROM t_sink WHERE C_BIGINT IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BIGINT");
        assertNull(rs.getBigDecimal(1), "C_BIGINT should be NULL");

        // Test NUMERIC NULL
        rs = stmt.executeQuery("SELECT C_NUMERIC FROM t_sink WHERE C_NUMERIC IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_NUMERIC");
        assertNull(rs.getBigDecimal(1), "C_NUMERIC should be NULL");
    }

    /**
     * Test that NULL DOUBLE is preserved (not converted to 0.0).
     */
    @Test
    void testNullDoublePreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_DOUBLE_PRECISION FROM t_sink WHERE C_DOUBLE_PRECISION IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_DOUBLE_PRECISION");
        rs.getDouble(1);
        assertTrue(rs.wasNull(), "C_DOUBLE_PRECISION should be NULL (not 0.0)");
    }

    /**
     * Test that NULL FLOAT is preserved.
     */
    @Test
    void testNullFloatPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_FLOAT FROM t_sink WHERE C_FLOAT IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_FLOAT");
        rs.getFloat(1);
        assertTrue(rs.wasNull(), "C_FLOAT should be NULL (not 0.0f)");
    }

    /**
     * Test that NULL DATE is preserved (not converted to epoch date).
     */
    @Test
    void testNullDatePreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_DATE FROM t_sink WHERE C_DATE IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_DATE");
        Date dateVal = rs.getDate(1);
        assertTrue(rs.wasNull() || dateVal == null, "C_DATE should be NULL (not epoch date)");
    }

    /**
     * Test that NULL TIMESTAMP is preserved.
     */
    @Test
    void testNullTimestampPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_TIMESTAMP_WITHOUT_TIMEZONE FROM t_sink WHERE C_TIMESTAMP_WITHOUT_TIMEZONE IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_TIMESTAMP_WITHOUT_TIMEZONE");
        Timestamp tsVal = rs.getTimestamp(1);
        assertTrue(rs.wasNull() || tsVal == null, "C_TIMESTAMP_WITHOUT_TIMEZONE should be NULL");
    }

    /**
     * Test that NULL VARCHAR is preserved (not converted to empty string).
     */
    @Test
    void testNullStringPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_CHARACTER_VAR FROM t_sink WHERE C_CHARACTER_VAR IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_CHARACTER_VAR");
        String strVal = rs.getString(1);
        assertTrue(rs.wasNull() || strVal == null, "C_CHARACTER_VAR should be NULL (not empty string)");
    }

    /**
     * Test that NULL BINARY is preserved (not converted to empty bytes).
     */
    @Test
    void testNullBinaryPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_BINARY_VAR FROM t_sink WHERE C_BINARY_VAR IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BINARY_VAR");
        byte[] bytesVal = rs.getBytes(1);
        assertTrue(rs.wasNull() || bytesVal == null, "C_BINARY_VAR should be NULL (not empty bytes)");
    }

    /**
     * Test that NULL BOOLEAN is preserved (not converted to false).
     */
    @Test
    void testNullBooleanPreserved() throws Exception {
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "t_source",
                "--sink-connect", sinkUrl,
                "--sink-table", "t_sink",
                "--jobs", "1"
        })));

        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_BOOLEAN FROM t_sink WHERE C_BOOLEAN IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BOOLEAN");
        rs.getBoolean(1);
        assertTrue(rs.wasNull(), "C_BOOLEAN should be NULL (not false)");
    }
}
