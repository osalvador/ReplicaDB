package org.replicadb.postgres;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Postgres2DB2Test {
    private static final Logger LOG = LogManager.getLogger(Postgres2DB2Test.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;
    private static final String COLUMN_LIST = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,"
            + "C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,"
            + "C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";

    private Connection db2Conn;
    private Connection postgresConn;

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();
    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();


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
        db2Conn.createStatement().execute("DELETE t_sink");
        this.db2Conn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
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
    void testPostgres2Db2DBComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2Db2DBCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2Db2DBIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2Db2DBIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2Db2DBCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2Db2DBCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST
        };

        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2Db2AutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "T_SINK_AUTOCREATE_POSTGRES2DB2";
        Assertions.assertFalse(tableExists(db2Conn, sinkTable), "Sink table should not exist before test");

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(db2Conn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(db2Conn, sinkTable));

        // Cleanup
        db2Conn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testPostgres2Db2AutoCreateCompleteAtomicMode() throws ParseException, IOException, SQLException {
        String sinkTable = "T_SINK_AUTOCREATE_POSTGRES2DB2_ATOMIC";
        Assertions.assertFalse(tableExists(db2Conn, sinkTable), "Sink table should not exist before test");

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(db2Conn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(db2Conn, sinkTable));

        // Cleanup
        db2Conn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testPostgres2Db2AutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "T_SINK"; // Use existing table

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create",
                "--source-columns", COLUMN_LIST,
                "--sink-columns", COLUMN_LIST,
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

}
