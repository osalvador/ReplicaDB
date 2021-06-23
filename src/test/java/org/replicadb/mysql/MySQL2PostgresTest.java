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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2PostgresTest {
    private static final Logger LOG = LogManager.getLogger(MySQL2PostgresTest.class);
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String MYSQL_SOURCE_FILE = "/mysql/mysql-source.sql";
    private static final String POSTGRES_SINK_FILE = "/sinks/pg-sink.sql";
    private static final String USER_PASSWD_DB = "replicadb";

    private Connection mysqlConn;
    private Connection postgresConn;

    @ClassRule
    private static final MySQLContainer mysql = new MySQLContainer("mysql:5.6")
            .withDatabaseName(USER_PASSWD_DB)
            .withUsername(USER_PASSWD_DB)
            .withPassword(USER_PASSWD_DB);

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:9.6")
            .withDatabaseName(USER_PASSWD_DB)
            .withUsername(USER_PASSWD_DB)
            .withPassword(USER_PASSWD_DB);

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        // Start the mysql container
        mysql.start();
        postgres.start();
        // Create tables
        /*MySQL*/
        Connection con = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        ScriptRunner runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + MYSQL_SOURCE_FILE)));
        LOG.info("Creating MySQL source tables");
        con.close();
        /*Postgres*/
        con = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + POSTGRES_SINK_FILE)));
        LOG.info("Creating Postgres sink tables");
        con.close();
    }

    @BeforeEach
    void before() throws SQLException {
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        postgresConn.createStatement().execute("TRUNCATE TABLE T_SINK");
        this.mysqlConn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
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
    void testPostgresConnection() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testMysqlInit() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("4096"));
    }

    @Test
    void testMySQL2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(4096,countSinkRows());
    }

    @Test
    void testMySQL2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(4096,countSinkRows());

    }

    @Test
    void testMySQL2PostgresIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(4096,countSinkRows());

    }

    @Test
    void testMySQL2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(4096,countSinkRows());
    }

    @Test
    void testMySQL2PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(4096,countSinkRows());
    }

    @Test
    void testMySQL2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(4096,countSinkRows());
    }
}