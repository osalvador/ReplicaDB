package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Oracle2OracleSdoGeomTest {
    private static final Logger LOG = LogManager.getLogger(Oracle2OracleSdoGeomTest.class);
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String ORACLE_SOURCE_FILE = "/oracle/oracle-sdo_geometry.sql";
    private static final int EXPECTED_ROWS = 4;

    private Connection oracleConn;

    @ClassRule
    public static final OracleContainer oracle = new OracleContainer("oracleinanutshell/oracle-xe-11g");

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        // Start the oracle container
        oracle.start();
        // Create tables
        /*Oracle*/
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(timeZone);
        Connection con = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
        ScriptRunner runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + ORACLE_SOURCE_FILE)));
        con.close();
    }

    @BeforeEach
    void before() throws SQLException {
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        oracleConn.createStatement().execute("TRUNCATE TABLE T_SINK");
        this.oracleConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        return count;
    }

    @Test
    void testOracleConnection() throws SQLException {
        Connection oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
        oracleConn.close();
    }

    @Test
    void testOracleInit() throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS,rows);
    }

    @Test
    void testOracle2OracleComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testOracle2OracleCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--sink-staging-schema", oracle.getUsername(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testOracle2OracleIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--sink-staging-schema", oracle.getUsername(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testOracle2OracleCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testOracle2OracleCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--sink-staging-schema", oracle.getUsername(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testOracle2OracleIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--sink-staging-schema", oracle.getUsername(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }
}