package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test StandardJDBCManager NULL handling for primitive types.
 * 
 * NOTE: This test class is currently DISABLED because MySQL uses MySQLManager (with LOAD DATA INFILE),
 * not StandardJDBCManager. StandardJDBCManager is only used as a fallback for unknown JDBC drivers.
 * 
 * NULL handling is already comprehensively tested in:
 * - OracleManagerNullHandlingTest
 * - SqliteManagerNullHandlingTest
 * 
 * The StandardJDBCManager code has been fixed to handle Types.VARBINARY/LONGVARBINARY null values,
 * but testing it requires a database that doesn't have a specific Manager implementation (e.g., Derby, H2).
 * 
 * TODO: Either remove this test or convert it to use an embedded database (Derby/H2) that actually
 * uses StandardJDBCManager.
 */
@Disabled("MySQL uses MySQLManager, not StandardJDBCManager - see class comment")
@Testcontainers
class StandardJDBCManagerNullHandlingTest {
    private static final Logger LOG = LogManager.getLogger(StandardJDBCManagerNullHandlingTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";

    private Connection mysqlConn;

    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        
        Statement stmt = mysqlConn.createStatement();
        
        // Recreate source table with explicit NULL allowance for TIMESTAMP and BINARY
        stmt.execute("DROP TABLE IF EXISTS t_source");
        stmt.execute(
            "CREATE TABLE t_source (" +
            "C_INTEGER INTEGER AUTO_INCREMENT," +
            "C_SMALLINT SMALLINT," +
            "C_BIGINT BIGINT," +
            "C_NUMERIC NUMERIC(65, 30)," +
            "C_DECIMAL DECIMAL(65, 30)," +
            "C_REAL REAL," +
            "C_DOUBLE_PRECISION DOUBLE PRECISION," +
            "C_FLOAT FLOAT," +
            "C_BINARY VARBINARY(35)," +
            "C_BINARY_VAR VARBINARY(255)," +
            "C_BINARY_LOB LONGBLOB," +
            "C_BOOLEAN BOOLEAN," +
            "C_CHARACTER CHAR(35)," +
            "C_CHARACTER_VAR VARCHAR(255)," +
            "C_CHARACTER_LOB TEXT," +
            "C_NATIONAL_CHARACTER NATIONAL CHARACTER(35)," +
            "C_NATIONAL_CHARACTER_VAR NVARCHAR(255)," +
            "C_DATE DATE," +
            "C_TIME_WITHOUT_TIMEZONE TIME," +
            "C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP NULL," +
            "C_TIME_WITH_TIMEZONE TIME," +
            "C_TIMESTAMP_WITH_TIMEZONE TIMESTAMP NULL," +
            "PRIMARY KEY (C_INTEGER)" +
            ")"
        );
        
        // Insert test data with some regular rows
        for (int i = 0; i < 10; i++) {
            stmt.execute(
                "INSERT INTO t_source (C_SMALLINT, C_BIGINT, C_NUMERIC, C_DECIMAL, C_REAL, " +
                "C_DOUBLE_PRECISION, C_FLOAT, C_BINARY, C_BINARY_VAR, C_BINARY_LOB, C_BOOLEAN, " +
                "C_CHARACTER, C_CHARACTER_VAR, C_CHARACTER_LOB, C_NATIONAL_CHARACTER, " +
                "C_NATIONAL_CHARACTER_VAR, C_DATE, C_TIME_WITHOUT_TIMEZONE, " +
                "C_TIMESTAMP_WITHOUT_TIMEZONE, C_TIME_WITH_TIMEZONE, C_TIMESTAMP_WITH_TIMEZONE) " +
                "VALUES (" + i + ", " + (i * 1000) + ", " + i + ".5, " + i + ".25, " + i + ".1, " +
                i + ".2, " + i + ".3, X'AABBCC', X'DDEEFF', X'112233', TRUE, " +
                "'char" + i + "', 'varchar" + i + "', 'text" + i + "', 'nchar" + i + "', " +
                "'nvarchar" + i + "', CURDATE(), CURTIME(), NOW(), CURTIME(), NOW())"
            );
        }
        
        // Insert NULL row (the row we're testing)
        stmt.execute(
            "INSERT INTO t_source (C_SMALLINT, C_BIGINT, C_NUMERIC, C_DECIMAL, C_REAL, " +
            "C_DOUBLE_PRECISION, C_FLOAT, C_BINARY, C_BINARY_VAR, C_BINARY_LOB, C_BOOLEAN, " +
            "C_CHARACTER, C_CHARACTER_VAR, C_CHARACTER_LOB, C_NATIONAL_CHARACTER, " +
            "C_NATIONAL_CHARACTER_VAR, C_DATE, C_TIME_WITHOUT_TIMEZONE, " +
            "C_TIMESTAMP_WITHOUT_TIMEZONE, C_TIME_WITH_TIMEZONE, C_TIMESTAMP_WITH_TIMEZONE) " +
            "VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " +
            "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        );
        
        // Recreate sink table with explicit NULL allowance for TIMESTAMP and BINARY
        stmt.execute("DROP TABLE IF EXISTS t_sink");
        stmt.execute(
            "CREATE TABLE t_sink (" +
            "C_INTEGER INTEGER," +
            "C_SMALLINT SMALLINT," +
            "C_BIGINT BIGINT," +
            "C_NUMERIC NUMERIC(65, 30)," +
            "C_DECIMAL DECIMAL(65, 30)," +
            "C_REAL REAL," +
            "C_DOUBLE_PRECISION DOUBLE PRECISION," +
            "C_FLOAT FLOAT," +
            "C_BINARY VARBINARY(35)," +
            "C_BINARY_VAR VARBINARY(255)," +
            "C_BINARY_LOB BLOB," +
            "C_BOOLEAN BOOLEAN," +
            "C_CHARACTER CHAR(35)," +
            "C_CHARACTER_VAR VARCHAR(255)," +
            "C_CHARACTER_LOB TEXT," +
            "C_NATIONAL_CHARACTER NATIONAL CHARACTER(35)," +
            "C_NATIONAL_CHARACTER_VAR NVARCHAR(255)," +
            "C_DATE DATE," +
            "C_TIME_WITHOUT_TIMEZONE TIME," +
            "C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP NULL," +
            "C_TIME_WITH_TIMEZONE TIME," +
            "C_TIMESTAMP_WITH_TIMEZONE TIMESTAMP NULL," +
            "C_INTERVAL_DAY TEXT," +
            "C_INTERVAL_YEAR TEXT," +
            "C_ARRAY TEXT," +
            "C_MULTIDIMENSIONAL_ARRAY TEXT," +
            "C_MULTISET TEXT," +
            "C_XML TEXT," +
            "C_JSON TEXT," +
            "PRIMARY KEY (C_INTEGER)" +
            ")"
        );
        stmt.close();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clean up test tables
        Statement stmt = this.mysqlConn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_source");
        stmt.execute("DROP TABLE IF EXISTS t_sink");
        stmt.close();
        this.mysqlConn.close();
    }

    /**
     * Test that NULL INTEGER is preserved (not converted to 0).
     * MySQL test data (mysql-source.sql) has a NULL test row.
     */
    @Test
    void testNullIntegerPreserved() throws Exception {
        // Replicate all data (includes NULL row)
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        // Verify NULL row exists
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_SMALLINT FROM t_sink WHERE C_SMALLINT IS NULL");
        assertTrue(rs.next(), "Should find at least one row with NULL C_SMALLINT");
        rs.getInt(1);
        assertTrue(rs.wasNull(), "C_SMALLINT should be NULL (not 0)");

        // Verify it's NOT stored as 0 (the bug behavior)
        rs = stmt.executeQuery("SELECT COUNT(*) FROM t_sink WHERE C_SMALLINT IS NULL");
        rs.next();
        int nullCount = rs.getInt(1);
        assertTrue(nullCount > 0, "Should have rows with NULL C_SMALLINT (not converted to 0)");
    }

    /**
     * Test that NULL BIGINT/NUMERIC is preserved.
     */
    @Test
    void testNullBigDecimalPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        
        // Test BIGINT NULL
        ResultSet rs = stmt.executeQuery("SELECT C_BIGINT FROM t_sink WHERE C_BIGINT IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BIGINT");
        rs.getBigDecimal(1);
        assertTrue(rs.wasNull(), "C_BIGINT should be NULL");

        // Test NUMERIC NULL
        rs = stmt.executeQuery("SELECT C_NUMERIC FROM t_sink WHERE C_NUMERIC IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_NUMERIC");
        rs.getBigDecimal(1);
        assertTrue(rs.wasNull(), "C_NUMERIC should be NULL");

        // Test DECIMAL NULL
        rs = stmt.executeQuery("SELECT C_DECIMAL FROM t_sink WHERE C_DECIMAL IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_DECIMAL");
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
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_DOUBLE_PRECISION FROM t_sink WHERE C_DOUBLE_PRECISION IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_DOUBLE_PRECISION");
        rs.getDouble(1);
        assertTrue(rs.wasNull(), "C_DOUBLE_PRECISION should be NULL (not 0.0)");
    }

    /**
     * Test that NULL FLOAT/REAL is preserved.
     */
    @Test
    void testNullFloatPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        
        // Test REAL NULL
        ResultSet rs = stmt.executeQuery("SELECT C_REAL FROM t_sink WHERE C_REAL IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_REAL");
        rs.getFloat(1);
        assertTrue(rs.wasNull(), "C_REAL should be NULL (not 0.0f)");

        // Test FLOAT NULL
        rs = stmt.executeQuery("SELECT C_FLOAT FROM t_sink WHERE C_FLOAT IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_FLOAT");
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
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
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
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        
        // Test TIMESTAMP WITHOUT TIMEZONE
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
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        
        // Test CHARACTER NULL
        ResultSet rs = stmt.executeQuery("SELECT C_CHARACTER FROM t_sink WHERE C_CHARACTER IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_CHARACTER");
        String strVal = rs.getString(1);
        assertTrue(rs.wasNull() || strVal == null, "C_CHARACTER should be NULL (not empty string)");

        // Test CHARACTER_VAR NULL
        rs = stmt.executeQuery("SELECT C_CHARACTER_VAR FROM t_sink WHERE C_CHARACTER_VAR IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_CHARACTER_VAR");
        strVal = rs.getString(1);
        assertTrue(rs.wasNull() || strVal == null, "C_CHARACTER_VAR should be NULL");
    }

    /**
     * Test that NULL BINARY is preserved (not converted to empty bytes).
     */
    @Test
    void testNullBinaryPreserved() throws Exception {
        // First verify NULL exists in source
        Statement checkStmt = mysqlConn.createStatement();
        ResultSet checkRs = checkStmt.executeQuery("SELECT COUNT(*) FROM t_source WHERE C_BINARY IS NULL");
        checkRs.next();
        int nullCount = checkRs.getInt(1);
        LOG.info("Rows with NULL C_BINARY in source: {}", nullCount);
        checkRs.close();
        
        checkRs = checkStmt.executeQuery("SELECT C_INTEGER, C_BINARY FROM t_source ORDER BY C_INTEGER DESC LIMIT 3");
        LOG.info("Last 3 rows in source:");
        while (checkRs.next()) {
            byte[] bytes = checkRs.getBytes(2);
            LOG.info("  C_INTEGER={}, C_BINARY={}, wasNull={}", 
                     checkRs.getInt(1), bytes, checkRs.wasNull());
        }
        checkRs.close();
        checkStmt.close();
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        
        // Check what's in sink
        ResultSet sinkCheckRs = stmt.executeQuery("SELECT COUNT(*) FROM t_sink WHERE C_BINARY IS NULL");
        sinkCheckRs.next();
        int sinkNullCount = sinkCheckRs.getInt(1);
        LOG.info("Rows with NULL C_BINARY in sink: {}", sinkNullCount);
        sinkCheckRs.close();
        
        // Test BINARY NULL
        ResultSet rs = stmt.executeQuery("SELECT C_BINARY FROM t_sink WHERE C_BINARY IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BINARY");
        byte[] bytesVal = rs.getBytes(1);
        assertTrue(rs.wasNull() || bytesVal == null, "C_BINARY should be NULL (not empty bytes)");

        // Test BINARY_VAR NULL
        rs = stmt.executeQuery("SELECT C_BINARY_VAR FROM t_sink WHERE C_BINARY_VAR IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BINARY_VAR");
        bytesVal = rs.getBytes(1);
        assertTrue(rs.wasNull() || bytesVal == null, "C_BINARY_VAR should be NULL");
    }

    /**
     * Test that NULL BOOLEAN is preserved (not converted to false).
     * StandardJDBCManager has explicit BOOLEAN handling.
     */
    @Test
    void testNullBooleanPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_BOOLEAN FROM t_sink WHERE C_BOOLEAN IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_BOOLEAN");
        rs.getBoolean(1);
        assertTrue(rs.wasNull(), "C_BOOLEAN should be NULL (not false)");
    }

    /**
     * Test that NULL NVARCHAR is preserved.
     * StandardJDBCManager has explicit NVARCHAR handling.
     */
    @Test
    void testNullNVarcharPreserved() throws Exception {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT C_NATIONAL_CHARACTER_VAR FROM t_sink WHERE C_NATIONAL_CHARACTER_VAR IS NULL");
        assertTrue(rs.next(), "Should find row with NULL C_NATIONAL_CHARACTER_VAR");
        String strVal = rs.getString(1);
        assertTrue(rs.wasNull() || strVal == null, "C_NATIONAL_CHARACTER_VAR should be NULL (not empty string)");
    }
}
