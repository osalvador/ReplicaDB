package org.replicadb.mariadb;

import org.apache.commons.cli.ParseException;
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
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.MariaDBContainer;
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
class MariaDB2DB2Test {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    private static final String COLUMN_LIST = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,"
            + "C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,"
            + "C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";

    private Connection mariadbConn;
    private Connection db2Conn;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        db2Conn.createStatement().execute("DELETE t_sink");
        this.mariadbConn.close();
        this.db2Conn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        return rs.getInt(1);
    }

    @Test
    void testMariaDbConnection() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("10.2"));
    }

    @Test
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("1"));
    }

    @Test
    void testMariaDb2Db2Complete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDb2Db2CompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDb2Db2Incremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDb2Db2CompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDb2Db2CompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDb2Db2IncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
