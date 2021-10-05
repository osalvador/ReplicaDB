package org.replicadb.mysql;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2MySQLTest {
    private static final Logger LOG = LogManager.getLogger(MySQL2MySQLTest.class);
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String MYSQL_SOURCE_FILE = "/mysql/mysql-source.sql";
    private static final String MYSQL_SINK_FILE = "/sinks/mysql-sink.sql";
    private static final String USER_PASSWD_DB = "replicadb";
    private static final int EXPECTED_ROWS = 4097;

    private Connection mysqlConn;
    private String mysqlJdbcUrl = "";

    @ClassRule
    private static final MySQLContainer mysql = (MySQLContainer) new MySQLContainer("mysql:5.6")
            .withDatabaseName(USER_PASSWD_DB)
            .withUsername(USER_PASSWD_DB)
            .withPassword(USER_PASSWD_DB)
            .withCommand("--local-infile=1");

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        // Start the mysql container
        mysql.start();
        // Create tables
        /*MySQL*/
        Connection con = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        ScriptRunner runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + MYSQL_SOURCE_FILE)));
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + MYSQL_SINK_FILE)));
        con.close();
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
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
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
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
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
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
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
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
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
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
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
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
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
}