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
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.MSSQLServerContainer;
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
class DB22SqlserverTest {
    private static final Logger LOG = LogManager.getLogger(DB22SqlserverTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    private static final String COLUMN_LIST = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,"
            + "C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,"
            + "C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE";

    private Connection db2Conn;
    private Connection sqlserverConn;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.db2Conn.close();
        this.sqlserverConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
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
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testSqlserverConnection() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testDb22SqlserverComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqlserverCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--sink-staging-schema", "dbo",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqlserverIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--sink-staging-schema", "dbo",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqlserverCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqlserverCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--sink-staging-schema", "dbo",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22SqlserverIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--sink-staging-schema", "dbo",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDB22SqlserverAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_db22sqlserver";
        Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", db2.getJdbcUrl(),
            "--source-user", db2.getUsername(),
            "--source-password", db2.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(sqlserverConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(sqlserverConn, sinkTable));

        // Cleanup
        sqlserverConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testDB22SqlserverAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_db22sqlserver_incr";
        Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", db2.getJdbcUrl(),
            "--source-user", db2.getUsername(),
            "--source-password", db2.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-staging-schema", "dbo",
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(sqlserverConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(sqlserverConn, sinkTable));

        // Test merge by running again
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countRows(sqlserverConn, sinkTable), "Row count should remain same after merge");

        // Cleanup
        sqlserverConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testDB22SqlserverAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", db2.getJdbcUrl(),
            "--source-user", db2.getUsername(),
            "--source-password", db2.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--source-columns", COLUMN_LIST,
            "--sink-columns", COLUMN_LIST,
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
