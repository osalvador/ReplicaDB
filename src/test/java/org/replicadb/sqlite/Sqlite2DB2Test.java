package org.replicadb.sqlite;

import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.config.ReplicadbSqliteFakeContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Sqlite2DB2Test {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    private static final String COLUMN_LIST = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,"
            + "C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,"
            + "C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,"
            + "C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";
        private static final String SOURCE_QUERY = "SELECT "
            + "C_INTEGER,"
            + "C_SMALLINT,"
            + "C_BIGINT,"
            + "CAST(C_NUMERIC / 1000000000000.0 AS DECIMAL(30,15)) AS C_NUMERIC,"
            + "CAST(C_DECIMAL / 1000000000000.0 AS DECIMAL(30,15)) AS C_DECIMAL,"
            + "CAST(C_REAL / 1000000000.0 AS DECIMAL(30,15)) AS C_REAL,"
            + "CAST(C_DOUBLE_PRECISION / 1000000000.0 AS DECIMAL(30,15)) AS C_DOUBLE_PRECISION,"
            + "CAST(C_FLOAT / 1000000000.0 AS DECIMAL(30,15)) AS C_FLOAT,"
            + "C_BINARY,"
            + "C_BINARY_VAR,"
            + "C_BINARY_LOB,"
            + "C_BOOLEAN,"
            + "substr(C_CHARACTER,1,35) AS C_CHARACTER,"
            + "substr(C_CHARACTER_VAR,1,255) AS C_CHARACTER_VAR,"
            + "substr(C_CHARACTER_LOB,1,4000) AS C_CHARACTER_LOB,"
            + "substr(C_NATIONAL_CHARACTER,1,35) AS C_NATIONAL_CHARACTER,"
            + "substr(C_NATIONAL_CHARACTER_VAR,1,255) AS C_NATIONAL_CHARACTER_VAR,"
            + "C_DATE,"
            + "substr(C_TIME_WITHOUT_TIMEZONE,1,100) AS C_TIME_WITHOUT_TIMEZONE,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE,"
            + "substr(C_TIME_WITH_TIMEZONE,1,100) AS C_TIME_WITH_TIMEZONE,"
            + "C_TIMESTAMP_WITH_TIMEZONE "
            + "FROM t_source";

    private Connection sqliteConn;
    private Connection db2Conn;

    @Rule
    public static ReplicadbSqliteFakeContainer sqlite = ReplicadbSqliteFakeContainer.getInstance();
    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.sqliteConn = DriverManager.getConnection(sqlite.getJdbcUrl());
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        db2Conn.createStatement().execute("DELETE t_sink");
        this.sqliteConn.close();
        this.db2Conn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        return rs.getInt(1);
    }

    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName.toUpperCase(), new String[]{"TABLE"})) {
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
    void testSqliteConnection() throws SQLException {
        Statement stmt = sqliteConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String result = rs.getString(1);
        assertTrue(result.contains("1"));
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
    void testSqlite2Db2Complete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", db2.getJdbcUrl(),
            "--sink-user", db2.getUsername(),
            "--sink-password", db2.getPassword(),
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2Db2CompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
            "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());

    }

    @Test
    void testSqlite2Db2Incremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
            "--mode", ReplicationMode.INCREMENTAL.getModeText(),
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());

    }

    @Test
    void testSqlite2Db2CompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
            "--jobs", "4",
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2Db2CompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
            "--jobs", "4",
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2Db2IncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
            "--jobs", "4",
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQLite JDBC driver reports TIME columns with incorrect JDBC type, causing type mapping issues. Auto-create from SQLite sources requires manual column specification.")
    @Test
    void testSqlite2DB2AutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "T_SINK_AUTOCREATE_SQLITE2DB2";
        Assertions.assertFalse(tableExists(db2Conn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", db2.getJdbcUrl(),
            "--sink-user", db2.getUsername(),
            "--sink-password", db2.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(db2Conn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(db2Conn, sinkTable));

        // Cleanup
        db2Conn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Disabled("SQLite JDBC driver reports TIME columns with incorrect JDBC type, causing type mapping issues. Auto-create from SQLite sources requires manual column specification.")
    @Test
    void testSqlite2DB2AutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "T_SINK_AUTOCREATE_SQLITE2DB2_INCR";
        Assertions.assertFalse(tableExists(db2Conn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", db2.getJdbcUrl(),
            "--sink-user", db2.getUsername(),
            "--sink-password", db2.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(db2Conn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(db2Conn, sinkTable));

        // Test merge by running again
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countRows(db2Conn, sinkTable), "Row count should remain same after merge");

        // Cleanup
        db2Conn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Disabled("SQLite JDBC driver reports TIME columns with incorrect JDBC type, causing type mapping issues. Auto-create from SQLite sources requires manual column specification.")
    @Test
    void testSqlite2DB2AutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "T_SINK"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", db2.getJdbcUrl(),
            "--sink-user", db2.getUsername(),
            "--sink-password", db2.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--source-query", SOURCE_QUERY,
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST,
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
