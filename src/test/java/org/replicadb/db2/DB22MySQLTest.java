package org.replicadb.db2;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22MySQLTest {
    private static final Logger LOG = LogManager.getLogger(DB22MySQLTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String COLUMN_LIST = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,"
            + "C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,"
            + "C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE";

    private Connection db2Conn;
    private Connection mysqlConn;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        mysqlConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.db2Conn.close();
        this.mysqlConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        return rs.getInt(1);
    }

    @Test
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testMysqlConnection() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String result = rs.getString(1);
        LOG.info(result);
        assertTrue(result.contains("1"));
    }

    @Test
    void testDb2Init() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int count = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, count);
    }

    @Test
    void testDb22MySQLComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MySQLCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--sink-staging-schema", mysql.getDatabaseName(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MySQLIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--sink-staging-schema", mysql.getDatabaseName(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MySQLCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MySQLCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--sink-staging-schema", mysql.getDatabaseName(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MySQLIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", mysql.getJdbcUrl(),
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--sink-staging-schema", mysql.getDatabaseName(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
