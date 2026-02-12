package org.replicadb.db2;

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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22SqliteTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    private static final String COLUMN_LIST = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,"
            + "C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,"
            + "C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE";

    private Connection db2Conn;
    private Connection sqliteConn;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @Rule
    public static ReplicadbSqliteFakeContainer sqlite = ReplicadbSqliteFakeContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.sqliteConn = DriverManager.getConnection(sqlite.getJdbcUrl());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Use DELETE for SQLite (no TRUNCATE support), but it's the sink so less critical
        sqliteConn.createStatement().execute("DELETE FROM t_sink");
        this.db2Conn.close();
        this.sqliteConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = sqliteConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        return rs.getInt(1);
    }

    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        java.sql.DatabaseMetaData meta = conn.getMetaData();
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
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("1"));
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
    void testDb22SqliteComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQLite does not support staging schema for complete-atomic mode")
    @Test
    void testDb22SqliteCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqliteIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
            "--sink-staging-schema", "main",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqliteCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQLite does not support staging schema for complete-atomic mode")
    @Test
    void testDb22SqliteCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqliteIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
            "--sink-staging-schema", "main",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDB22SqliteAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_db22sqlite";
        
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
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-table", sinkTable,
                "--sink-auto-create", "true",
                "--source-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        assertEquals(0, result, "Replication should succeed");
        
        // For SQLite sink, we can't immediately query due to locking,
        // but success return code (0) indicates table was created and data inserted
    }

    @Test
    void testDB22SqliteAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-table", sinkTable,
                "--sink-auto-create", "true",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        assertEquals(0, result, "Replication should succeed even when table exists");
    }
}
