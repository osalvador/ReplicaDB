package org.replicadb.mariadb;

import org.apache.commons.cli.ParseException;
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
class MariaDB2PostgresTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    // Skip BINARY and REAL/FLOAT columns due to binary COPY format incompatibilities
    private static final String SINK_COLUMNS = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL," +
            "C_BOOLEAN," +
            "C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
            "C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
            "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE," +
            "C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";
    private static final String SOURCE_COLUMNS = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL," +
            "C_BOOLEAN," +
            "C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
            "C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
            "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE," +
            "C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";

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
        postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.mariadbConn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        return rs.getInt(1);
    }

    @Test
    void testMariadbVersion102() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("10.2"));
    }

    @Test
    void testPostgresConnection() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("1"));
    }

    @Test
    void testMariadbInit() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        int count = rs.getInt(1);
        assertEquals(EXPECTED_ROWS,count);
    }

    @Test
    void testMariadb2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMariadb2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testMariadb2PostgresIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testMariadb2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMariadb2PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMariadb2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    // Helper methods for auto-create tests
    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName.toUpperCase(), new String[]{"TABLE"})) {
            if (rs.next()) return true;
        }
        try (ResultSet rs = meta.getTables(null, null, tableName.toLowerCase(), new String[]{"TABLE"})) {
            if (rs.next()) return true;
        }
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
    void testMariaDB2PostgresAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_mariadb2pg";
        
        // Verify table doesn't exist
        Assertions.assertFalse(tableExists(postgresConn, sinkTable), "Sink table should not exist before test");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-columns", SINK_COLUMNS,
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify table was created and populated
        assertTrue(tableExists(postgresConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(EXPECTED_ROWS, countRows(postgresConn, sinkTable));
        
        // Cleanup
        postgresConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testMariaDB2PostgresAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_incremental_mariadb2pg";
        
        // Verify table doesn't exist
        Assertions.assertFalse(tableExists(postgresConn, sinkTable), "Sink table should not exist before test");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", sinkTable,
                "--sink-columns", SINK_COLUMNS,
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify table was created with primary key
        assertTrue(tableExists(postgresConn, sinkTable), "Sink table should exist after auto-create");
        DatabaseMetaData meta = postgresConn.getMetaData();
        ResultSet pks = meta.getPrimaryKeys(null, "public", sinkTable);
        boolean hasResults = pks.next();
        if (!hasResults) {
            pks.close();
            pks = meta.getPrimaryKeys(null, "public", sinkTable.toLowerCase());
            hasResults = pks.next();
        }
        assertTrue(hasResults, "Table should have a primary key");
        String pkColumn = pks.getString("COLUMN_NAME");
        pks.close();
        assertEquals(EXPECTED_ROWS, countRows(postgresConn, sinkTable));
        
        // Run again to test merge functionality
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countRows(postgresConn, sinkTable), "Row count should remain the same after merge");
        
        // Cleanup
        postgresConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Test
    void testMariaDB2PostgresAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        // Use existing t_sink table
        assertTrue(tableExists(postgresConn, "t_sink"), "t_sink table should exist");
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", "t_sink",
                "--sink-columns", SINK_COLUMNS,
                "--sink-auto-create", "true",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
