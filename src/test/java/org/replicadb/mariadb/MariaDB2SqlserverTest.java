package org.replicadb.mariadb;

import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MariaDB2SqlserverTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection mariadbConn;
    private Connection sqlserverConn;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.mariadbConn.close();
        this.sqlserverConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        int count = rs.getInt(1);
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
    void testMariadbVersion102() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("10.2"));
    }

    @Test
    void testSqlserverVersion2019() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@VERSION");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("2019"));
    }

    @Test
    void testMariaDB2SqlserverComplete() throws ParseException, IOException, SQLException {
        // Exclude C_NUMERIC and C_DECIMAL (precision 65 > SQL Server max 38)
        // Must specify both source and sink columns for correct positional mapping
        // SQL Server sink uses lowercase column names
        String sourceColumns = "C_INTEGER,C_SMALLINT,C_BIGINT,C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
                "C_BOOLEAN,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
                "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE";
        String sinkColumns = "c_integer,c_smallint,c_bigint,c_real,c_double_precision,c_float," +
                "c_boolean,c_character,c_character_var,c_character_lob,c_national_character,c_national_character_var," +
                "c_date,c_time_without_timezone,c_timestamp_without_timezone";
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--source-columns", sourceColumns,
                "--sink-columns", sinkColumns
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support complete-atomic mode with staging schema - requires schema creation")
    @Test
    void testMariaDB2SqlserverCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--sink-staging-schema", "dbo",
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
    @Test
    void testMariaDB2SqlserverIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--sink-staging-schema", "dbo",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2SqlserverCompleteParallel() throws ParseException, IOException, SQLException {
        // Exclude C_NUMERIC and C_DECIMAL (precision 65 > SQL Server max 38)
        // Must specify both source and sink columns for correct positional mapping
        // SQL Server sink uses lowercase column names
        String sourceColumns = "C_INTEGER,C_SMALLINT,C_BIGINT,C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
                "C_BOOLEAN,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
                "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE";
        String sinkColumns = "c_integer,c_smallint,c_bigint,c_real,c_double_precision,c_float," +
                "c_boolean,c_character,c_character_var,c_character_lob,c_national_character,c_national_character_var," +
                "c_date,c_time_without_timezone,c_timestamp_without_timezone";
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--source-columns", sourceColumns,
                "--sink-columns", sinkColumns,
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support complete-atomic mode with staging schema - requires schema creation")
    @Test
    void testMariaDB2SqlserverCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--sink-staging-schema", "dbo",
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
    @Test
    void testMariaDB2SqlserverIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--sink-staging-schema", "dbo",
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2SqlserverAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_mariadb2sqlserver";
        Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", mariadb.getJdbcUrl(),
            "--source-user", mariadb.getUsername(),
            "--source-password", mariadb.getPassword(),
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

    @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
    @Test
    void testMariaDB2SqlserverAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_mariadb2sqlserver_incr";
        Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", mariadb.getJdbcUrl(),
            "--source-user", mariadb.getUsername(),
            "--source-password", mariadb.getPassword(),
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
    void testMariaDB2SqlserverAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table
        // Exclude C_NUMERIC and C_DECIMAL (precision 65 > SQL Server max 38)
        String sourceColumns = "C_INTEGER,C_SMALLINT,C_BIGINT,C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
                "C_BOOLEAN,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
                "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE";
        String sinkColumns = "c_integer,c_smallint,c_bigint,c_real,c_double_precision,c_float," +
                "c_boolean,c_character,c_character_var,c_character_lob,c_national_character,c_national_character_var," +
                "c_date,c_time_without_timezone,c_timestamp_without_timezone";

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", mariadb.getJdbcUrl(),
            "--source-user", mariadb.getUsername(),
            "--source-password", mariadb.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--source-columns", sourceColumns,
            "--sink-columns", sinkColumns,
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
