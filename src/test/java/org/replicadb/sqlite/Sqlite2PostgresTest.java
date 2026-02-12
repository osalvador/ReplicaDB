package org.replicadb.sqlite;

import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.replicadb.config.ReplicadbSqliteFakeContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Sqlite2PostgresTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;

    private Connection sqliteConn;
    private Connection postgresConn;

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();
    @Rule
    public static ReplicadbSqliteFakeContainer sqlite = ReplicadbSqliteFakeContainer.getInstance();

    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        this.sqliteConn = DriverManager.getConnection(sqlite.getJdbcUrl());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.sqliteConn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        return rs.getInt(1);
    }

    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName.toLowerCase(), new String[]{"TABLE"})) {
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
    void testSqlite2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Test
    void testSqlite2PostgresIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Test
    void testSqlite2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlite.getJdbcUrl(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Disabled("SQLite JDBC driver reports TIME columns with incorrect JDBC type, causing type mapping issues. Auto-create from SQLite sources requires manual column specification.")
    @Test
    void testSqlite2PostgresAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlite2postgres";
        Assertions.assertFalse(tableExists(postgresConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        Assertions.assertTrue(tableExists(postgresConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(postgresConn, sinkTable));

        // Cleanup
        postgresConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Disabled("SQLite JDBC driver reports TIME columns with incorrect JDBC type, causing type mapping issues. Auto-create from SQLite sources requires manual column specification.")
    @Test
    void testSqlite2PostgresAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlite2postgres_incr";
        Assertions.assertFalse(tableExists(postgresConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        Assertions.assertTrue(tableExists(postgresConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(postgresConn, sinkTable));

        // Cleanup
        postgresConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Disabled("SQLite JDBC driver reports TIME columns with incorrect JDBC type, causing type mapping issues. Auto-create from SQLite sources requires manual column specification.")
    @Test
    void testSqlite2PostgresAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testSqlite2PostgresAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlite2postgres_incr";
        Assertions.assertFalse(tableExists(postgresConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", sinkTable,
            "--sink-staging-schema", "public",
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        Assertions.assertTrue(tableExists(postgresConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(postgresConn, sinkTable));

        // Test merge by running again
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countRows(postgresConn, sinkTable), "Row count should remain same after merge");

        // Cleanup
        postgresConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testSqlite2PostgresAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlite.getJdbcUrl(),
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }
}
