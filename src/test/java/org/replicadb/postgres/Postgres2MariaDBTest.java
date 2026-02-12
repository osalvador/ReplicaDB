package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Postgres2MariaDBTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2MariaDBTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;

    private Connection mariadbConn;
    private Connection postgresConn;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();

    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        mariadbConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.mariadbConn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
    }


    @Test
    void testMariadbVersion102() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("10.2"));
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
    void testMariaDBInit() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testPostgres2MariaDBComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2MariaDBCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Test
    void testPostgres2MariaDBIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Test
    void testPostgres2MariaDBCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2MariaDBCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    // Helper methods for auto-create tests
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
    void testPostgres2MariaDBAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_pg2mariadb";
        
        // Verify table doesn't exist
        Assertions.assertFalse(tableExists(mariadbConn, sinkTable), "Sink table should not exist before test");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-table", sinkTable,
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify table was created and populated
        assertTrue(tableExists(mariadbConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(mariadbConn, sinkTable));
        LOG.info("Successfully replicated {} rows to auto-created MariaDB table", TOTAL_SINK_ROWS);
        
        // Cleanup
        mariadbConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testPostgres2MariaDBAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_incremental_pg2mariadb";
        
        // Verify table doesn't exist
        Assertions.assertFalse(tableExists(mariadbConn, sinkTable), "Sink table should not exist before test");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-table", sinkTable,
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify table was created with primary key
        assertTrue(tableExists(mariadbConn, sinkTable), "Sink table should exist after auto-create");
        DatabaseMetaData meta = mariadbConn.getMetaData();
        ResultSet pks = meta.getPrimaryKeys(null, null, sinkTable);
        assertTrue(pks.next(), "Table should have a primary key");
        String pkColumn = pks.getString("COLUMN_NAME");
        LOG.info("Primary key columns: {}", pkColumn);
        assertEquals(TOTAL_SINK_ROWS, countRows(mariadbConn, sinkTable));
        
        // Run again to test merge functionality
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countRows(mariadbConn, sinkTable), "Row count should remain the same after merge");
        LOG.info("Incremental mode merge successful - row count unchanged: {}", TOTAL_SINK_ROWS);
        
        // Cleanup
        mariadbConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testPostgres2MariaDBAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        // Use existing t_sink table
        assertTrue(tableExists(mariadbConn, "t_sink"), "t_sink table should exist");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-table", "t_sink",
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
        LOG.info("Auto-create correctly skipped for existing table, {} rows replicated", TOTAL_SINK_ROWS);
    }
}