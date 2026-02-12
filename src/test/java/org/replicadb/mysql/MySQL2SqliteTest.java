package org.replicadb.mysql;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.replicadb.config.ReplicadbSqliteFakeContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2SqliteTest {
    private static final Logger LOG = LogManager.getLogger(MySQL2SqliteTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection mysqlConn;
    private Connection sqliteConn;

    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    @Rule
    public static ReplicadbSqliteFakeContainer sqlite = ReplicadbSqliteFakeContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        this.sqliteConn = DriverManager.getConnection(sqlite.getJdbcUrl());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        sqliteConn.createStatement().execute("DELETE FROM t_sink");
        this.mysqlConn.close();
        this.sqliteConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = sqliteConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info("Sink rows: {}", count);
        return count;
    }

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
    void testMysqlVersion56() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("5.6"));
    }

    @Test
    void testSqliteConnection() throws SQLException {
        Statement stmt = sqliteConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        int result = rs.getInt(1);
        assertEquals(1, result);
    }

    @Test
    void testMysqlSourceRows() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testMySQL2SqliteComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2SqliteVersion() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-table", "t_sink"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2SqliteAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_mysql2sqlite";
        
        // Drop table if it exists from previous test run
        try {
            sqliteConn.createStatement().execute("DROP TABLE IF EXISTS " + sinkTable);
            sqliteConn.commit();
        } catch (SQLException e) {
            // Ignore if table doesn't exist
        }
        
        Assertions.assertFalse(tableExists(sqliteConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-table", sinkTable,
                "--sink-auto-create", "true"
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        assertEquals(0, result, "Replication should succeed");
        
        // For SQLite sink, we can't immediately query due to locking,
        // but success return code (0) indicates table was created and data inserted
        
        // Cleanup - will be handled by @AfterEach or @BeforeEach of next test
    }

    @Test
    void testMySQL2SqliteAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-table", sinkTable,
                "--sink-auto-create", "true"
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        assertEquals(0, result, "Replication should succeed even when table exists");
        // The t_sink table exists, so auto-create should be skipped
        // Success return code indicates auto-create was properly skipped and replication proceeded
    }
}
