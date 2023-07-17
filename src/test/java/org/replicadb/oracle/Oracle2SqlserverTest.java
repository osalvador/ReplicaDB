package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Oracle2SqlserverTest {
    private static final Logger LOG = LogManager.getLogger(Oracle2SqlserverTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String SOURCE_COLUMNS = "c_integer,c_smallint,c_bigint,to_char(c_numeric),to_char(c_decimal),to_char(c_real),to_char(c_double_precision),to_char(c_float),c_binary,c_binary_var,c_boolean,c_character,c_character_var,c_national_character,c_national_character_var,c_date,c_timestamp_without_timezone";
    
    private static final String SINK_COLUMNS = "c_integer,c_smallint,c_bigint,c_numeric           ,c_decimal         ,c_real           ,c_double_precision        ,c_float         ,c_binary,c_binary_var,c_boolean,c_character,c_character_var,c_national_character,c_national_character_var,c_date,c_timestamp_without_timezone";
    


    private Connection oracleConn;
    private Connection sqlserverConn;

    @Rule
    public static OracleContainer oracle = ReplicadbOracleContainer.getInstance();

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();

    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.oracleConn.close();
        this.sqlserverConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        return rs.getInt(1);
    }
    
    @Test
    void testOracleConnection () throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testSqlserverVersion2017() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@VERSION");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("2017"));
    }

    @Test
    void testOracleInit() throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        int count = rs.getInt(1);
        assertEquals(EXPECTED_ROWS,count);
    }

    @Test
    void testOracle2SqlserverComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testOracle2SqlserverCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--sink-staging-schema", sqlserver.getDatabaseName(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testOracle2SqlserverIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--sink-staging-schema", sqlserver.getDatabaseName(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testOracle2SqlserverCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testOracle2SqlserverCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--sink-staging-schema", sqlserver.getDatabaseName(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testOracle2SqlserverIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--sink-staging-schema", sqlserver.getDatabaseName(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }
}