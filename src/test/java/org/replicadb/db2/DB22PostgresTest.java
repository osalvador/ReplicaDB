package org.replicadb.db2;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22PostgresTest {
    private static final Logger LOG = LogManager.getLogger(DB22PostgresTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private Connection db2Conn;
    private Connection postgresConn;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();

    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.db2Conn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        return rs.getInt(1);
    }

    @Test
    void testDb2Connection () throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
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
    void testDb2Init() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        int count = rs.getInt(1);
        assertEquals(EXPECTED_ROWS,count);
    }

    @Test
    void testDb22PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        // Using Standard JDBC Manager
        Properties sourceConnectionParams = new Properties();
        sourceConnectionParams.setProperty("driver", "com.ibm.db2.jcc.DB2Driver");
        options.setSourceConnectionParams(sourceConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testDb22PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(1, ReplicaDB.processReplica(options));


    }
//
//    @Test
//    void testDb22PostgresIncremental() throws ParseException, IOException, SQLException {
//        String[] args = {
//                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
//                "--source-connect", db2.getJdbcUrl(),
//                "--source-user", db2.getUsername(),
//                "--source-password", db2.getPassword(),
//                "--sink-connect", postgres.getJdbcUrl(),
//                "--sink-user", postgres.getUsername(),
//                "--sink-password", postgres.getPassword(),
//                "--mode", ReplicationMode.INCREMENTAL.getModeText()
//        };
//        ToolOptions options = new ToolOptions(args);
//        assertEquals(0, ReplicaDB.processReplica(options));
//        assertEquals(EXPECTED_ROWS,countSinkRows());
//
//    }
//
//    @Test
//    void testDb22PostgresCompleteParallel() throws ParseException, IOException, SQLException {
//        String[] args = {
//                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
//                "--source-connect", db2.getJdbcUrl(),
//                "--source-user", db2.getUsername(),
//                "--source-password", db2.getPassword(),
//                "--sink-connect", postgres.getJdbcUrl(),
//                "--sink-user", postgres.getUsername(),
//                "--sink-password", postgres.getPassword(),
//                "--jobs", "4"
//        };
//        ToolOptions options = new ToolOptions(args);
//        assertEquals(0, ReplicaDB.processReplica(options));
//        assertEquals(EXPECTED_ROWS,countSinkRows());
//    }
//
//    @Test
//    void testDb22PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
//        String[] args = {
//                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
//                "--source-connect", db2.getJdbcUrl(),
//                "--source-user", db2.getUsername(),
//                "--source-password", db2.getPassword(),
//                "--sink-connect", postgres.getJdbcUrl(),
//                "--sink-user", postgres.getUsername(),
//                "--sink-password", postgres.getPassword(),
//                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
//                "--jobs", "4"
//        };
//        ToolOptions options = new ToolOptions(args);
//        assertEquals(0, ReplicaDB.processReplica(options));
//        assertEquals(EXPECTED_ROWS,countSinkRows());
//    }
//
//    @Test
//    void testDb22PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
//        String[] args = {
//                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
//                "--source-connect", db2.getJdbcUrl(),
//                "--source-user", db2.getUsername(),
//                "--source-password", db2.getPassword(),
//                "--sink-connect", postgres.getJdbcUrl(),
//                "--sink-user", postgres.getUsername(),
//                "--sink-password", postgres.getPassword(),
//                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
//                "--jobs", "4"
//        };
//        ToolOptions options = new ToolOptions(args);
//        assertEquals(0, ReplicaDB.processReplica(options));
//        assertEquals(EXPECTED_ROWS,countSinkRows());
//    }
}