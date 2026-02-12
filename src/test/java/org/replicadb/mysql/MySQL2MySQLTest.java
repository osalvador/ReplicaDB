package org.replicadb.mysql;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2MySQLTest {
    private static final Logger LOG = LogManager.getLogger(MySQL2MySQLTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection mysqlConn;
    private String mysqlJdbcUrl = "";

    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        // Set JDBC URL
        this.mysqlJdbcUrl = mysql.getJdbcUrl() ;
        this.mysqlConn = DriverManager.getConnection(mysqlJdbcUrl, mysql.getUsername(), mysql.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        mysqlConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.mysqlConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
    }

    public int countSinkNullRows() throws SQLException {
        String sqlQuery = "select count(*) " +
                "from t_sink " +
                "where C_SMALLINT is null ";
                /*" and "+
                "C_BIGINT is null and C_NUMERIC is null and C_DECIMAL is null and C_REAL is null and " +
                "C_DOUBLE_PRECISION is null and C_FLOAT is null and C_BINARY is null and C_BINARY_VAR is null and " +
                "C_BINARY_LOB is null and C_BOOLEAN is null and C_CHARACTER is null and C_CHARACTER_VAR is null and " +
                "C_CHARACTER_LOB is null and C_NATIONAL_CHARACTER is null and C_NATIONAL_CHARACTER_VAR is null and C_DATE is null and " +
                "C_TIME_WITHOUT_TIMEZONE is null and C_TIMESTAMP_WITHOUT_TIMEZONE is null and C_TIME_WITH_TIMEZONE is null and " +
                "C_TIMESTAMP_WITH_TIMEZONE is null";*/

        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlQuery);
        //ReplicaDB.printResultSet(rs);
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
    }


    @Test
    void testMysqlVersion56() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("5.6"));
    }

    @Test
    void testMysqlConnection() throws SQLException {
        Connection mysqlConn = DriverManager.getConnection(mysqlJdbcUrl, mysql.getUsername(), mysql.getPassword());
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
        mysqlConn.close();
    }

    @Test
    void testMysqlInit() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testMySQL2MySQLComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
        assertEquals(1, countSinkNullRows(),"There must be a row with all its values set to null");
    }

    @Test
    void testMySQL2MySQLCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-staging-schema", mysql.getUsername(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());

    }

    @Test
    void testMySQL2MySQLIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-staging-schema", mysql.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());

    }

    @Test
    void testMySQL2MySQLCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2MySQLCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-staging-schema", mysql.getUsername(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2MySQLIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-staging-schema", mysql.getUsername(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    // Helper methods for auto-create tests
    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    private int countRows(Connection conn, String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
        return rs.getInt(1);
    }

    @Test
    void testMySQL2MySQLAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_mysql2mysql";
        
        // Verify table doesn't exist
        Assertions.assertFalse(tableExists(mysqlConn, sinkTable), "Sink table should not exist before test");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify table was created and populated
        assertTrue(tableExists(mysqlConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(mysqlConn, sinkTable));
        LOG.info("Successfully replicated {} rows to auto-created MySQL table", EXPECTED_ROWS);
        
        // Cleanup
        mysqlConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testMySQL2MySQLAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_incremental_mysql2mysql";
        
        // Verify table doesn't exist
        Assertions.assertFalse(tableExists(mysqlConn, sinkTable), "Sink table should not exist before test");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-table", sinkTable,
                "--sink-staging-schema", mysql.getDatabaseName(),
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify table was created with primary key
        assertTrue(tableExists(mysqlConn, sinkTable), "Sink table should exist after auto-create");
        DatabaseMetaData meta = mysqlConn.getMetaData();
        ResultSet pks = meta.getPrimaryKeys(null, null, sinkTable);
        assertTrue(pks.next(), "Table should have a primary key");
        String pkColumn = pks.getString("COLUMN_NAME");
        LOG.info("Primary key columns: {}", pkColumn);
        assertEquals(EXPECTED_ROWS, countRows(mysqlConn, sinkTable));
        
        // Run again to test merge functionality
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countRows(mysqlConn, sinkTable), "Row count should remain the same after merge");
        LOG.info("Incremental mode merge successful - row count unchanged: {}", EXPECTED_ROWS);
        
        // Cleanup
        mysqlConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testMySQL2MySQLAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        // Use existing t_sink table
        assertTrue(tableExists(mysqlConn, "t_sink"), "t_sink table should exist");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-table", "t_sink",
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
        LOG.info("Auto-create correctly skipped for existing table, {} rows replicated", EXPECTED_ROWS);
    }
}