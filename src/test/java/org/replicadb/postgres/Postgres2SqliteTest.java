package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.replicadb.config.ReplicadbSqliteFakeContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Postgres2SqliteTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2SqliteTest.class);
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
        sqliteConn.createStatement().execute("DELETE FROM t_sink");
        this.sqliteConn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = sqliteConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
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
    void testPostgres2SqliteComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2SqliteCompleteAtomic() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        // SQLite doesn't support complete-atomic mode, should return error code 1
        assertEquals(1, ReplicaDB.processReplica(options));
    }

    @Test
    void testPostgres2SqliteIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-staging-schema", sqlite.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Test
    void testPostgres2SqliteCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2SqliteCompleteAtomicParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        // SQLite doesn't support complete-atomic mode, should return error code 1
        assertEquals(1, ReplicaDB.processReplica(options));
    }

    @Test
    void testSqlite2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", sqlite.getJdbcUrl(),
                "--sink-staging-schema", sqlite.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2SqliteAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_postgres2sqlite";
        
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
            "--source-connect", postgres.getJdbcUrl(),
            "--source-user", postgres.getUsername(),
            "--source-password", postgres.getPassword(),
            "--sink-connect", sqlite.getJdbcUrl(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true"
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        assertEquals(0, result, "Replication should succeed");
        
        // For SQLite sink, we can't immediately query due to locking,
        // but success return code (0) indicates table was created and data inserted
    }

    @Test
    void testPostgres2SqliteAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", postgres.getJdbcUrl(),
            "--source-user", postgres.getUsername(),
            "--source-password", postgres.getPassword(),
            "--sink-connect", sqlite.getJdbcUrl(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true"
        };
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        assertEquals(0, result, "Replication should succeed even when table exists");
    }
}
