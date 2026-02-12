package org.replicadb.sqlserver;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Sqlserver2MySQLTest {
    private static final Logger LOG = LogManager.getLogger(Sqlserver2MySQLTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection sqlserverConn;
    private Connection mysqlConn;

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();
    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        mysqlConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.sqlserverConn.close();
        this.mysqlConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        return rs.getInt(1);
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
    void testSqlserverVersion2019() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@VERSION");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("2019"));
    }

    @Test
    void testMysqlVersion() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("5.6"));
    }

    @Test
    void testSqlserverSourceRows() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testSqlserver2MySQLComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2MySQLCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-staging-schema", "test",
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2MySQLIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-staging-schema", "test",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2MySQLCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2MySQLAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlserver2mysql";
        Assertions.assertFalse(tableExists(mysqlConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlserver.getJdbcUrl(),
            "--source-user", sqlserver.getUsername(),
            "--source-password", sqlserver.getPassword(),
            "--sink-connect", mysql.getJdbcUrl(),
            "--sink-user", mysql.getUsername(),
            "--sink-password", mysql.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(mysqlConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(mysqlConn, sinkTable));

        // Cleanup
        mysqlConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testSqlserver2MySQLAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlserver2mysql_incr";
        Assertions.assertFalse(tableExists(mysqlConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlserver.getJdbcUrl(),
            "--source-user", sqlserver.getUsername(),
            "--source-password", sqlserver.getPassword(),
            "--sink-connect", mysql.getJdbcUrl(),
            "--sink-user", mysql.getUsername(),
            "--sink-password", mysql.getPassword(),
            "--sink-table", sinkTable,
            "--sink-staging-schema", "test",
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(mysqlConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(mysqlConn, sinkTable));

        // Test merge by running again
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countRows(mysqlConn, sinkTable), "Row count should remain same after merge");

        // Cleanup
        mysqlConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testSqlserver2MySQLAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlserver.getJdbcUrl(),
            "--source-user", sqlserver.getUsername(),
            "--source-password", sqlserver.getPassword(),
            "--sink-connect", mysql.getJdbcUrl(),
            "--sink-user", mysql.getUsername(),
            "--sink-password", mysql.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
