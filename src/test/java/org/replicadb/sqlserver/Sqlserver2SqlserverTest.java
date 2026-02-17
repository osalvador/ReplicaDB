package org.replicadb.sqlserver;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Sqlserver2SqlserverTest {
    private static final Logger LOG = LogManager.getLogger(Sqlserver2SqlserverTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;
    private static final String COLUMNS ="c_integer,c_smallint,c_bigint,c_numeric,c_decimal,c_real,c_double_precision,c_float,c_binary,c_binary_var,c_binary_lob,c_boolean,c_character,c_character_var,c_character_lob,c_national_character,c_national_character_var,c_date,c_time_without_timezone,c_timestamp_without_timezone,c_xml";

    private Connection sqlserverSourceConn;
    private Connection sqlserverConn;

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlseverSource = ReplicadbSqlserverContainer.getInstance();
    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();


    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
        this.sqlserverSourceConn = DriverManager.getConnection(sqlseverSource.getJdbcUrl(), sqlseverSource.getUsername(), sqlseverSource.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.sqlserverConn.close();
        this.sqlserverSourceConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
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
    void testSqlserverVersion2019() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@VERSION");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("2019"));
    }

    @Test
    void testSqlserver2SqlserverComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlseverSource.getJdbcUrl(),
                "--source-user", sqlseverSource.getUsername(),
                "--source-password", sqlseverSource.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--source-columns", COLUMNS,
                "--sink-columns", COLUMNS,
                "--fetch-size", "1"
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support complete-atomic mode with staging schema - requires schema creation")
    @Test
    void testSqlserver2SqlserverCompleteAtomic () throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlseverSource.getJdbcUrl(),
            "--source-user", sqlseverSource.getUsername(),
            "--source-password", sqlseverSource.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-staging-schema", sqlserver.getDatabaseName(),
            "--source-columns", COLUMNS,
            "--sink-columns", COLUMNS,
            "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
    @Test
    void testSqlserver2SqlserverIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlseverSource.getJdbcUrl(),
                "--source-user", sqlseverSource.getUsername(),
                "--source-password", sqlseverSource.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--sink-staging-schema", sqlserver.getDatabaseName(),
                "--source-columns", COLUMNS,
                "--sink-columns", COLUMNS,
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Disabled("Parallel jobs with XML columns require per-job batch size configuration. " +
            "SQL Server 2019 requires fetch.size=1 for XML replication, but --fetch-size applies globally. " +
            "Need to implement per-job batch size control. Note: This limitation is SQL Server 2019 specific. " +
            "See: https://github.com/osalvador/ReplicaDB/issues/240")
    @Test
    void testSqlserver2SqlserverCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlseverSource.getJdbcUrl(),
                "--source-user", sqlseverSource.getUsername(),
                "--source-password", sqlseverSource.getPassword(),
                "--sink-connect", sqlserver.getJdbcUrl(),
                "--sink-user", sqlserver.getUsername(),
                "--sink-password", sqlserver.getPassword(),
                "--source-columns", COLUMNS,
                "--sink-columns", COLUMNS,
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support complete-atomic mode with staging schema - requires schema creation")
    @Test
    void testSqlserver2SqlserverCompleteAtomicParallel () throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlseverSource.getJdbcUrl(),
            "--source-user", sqlseverSource.getUsername(),
            "--source-password", sqlseverSource.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-staging-schema", sqlserver.getDatabaseName(),
            "--source-columns", COLUMNS,
            "--sink-columns", COLUMNS,
            "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
            "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
    @Test
    void testSqlserver2SqlserverIncrementalParallel () throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlseverSource.getJdbcUrl(),
            "--source-user", sqlseverSource.getUsername(),
            "--source-password", sqlseverSource.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-staging-schema", sqlserver.getDatabaseName(),
            "--source-columns", COLUMNS,
            "--sink-columns", COLUMNS,
            "--mode", ReplicationMode.INCREMENTAL.getModeText(),
            "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2SqlserverAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlserver2sqlserver";
        Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlseverSource.getJdbcUrl(),
            "--source-user", sqlseverSource.getUsername(),
            "--source-password", sqlseverSource.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.COMPLETE.getModeText(),
            "--fetch-size", "1"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(sqlserverConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(sqlserverConn, sinkTable));

        // Cleanup
        sqlserverConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
    @Test
    void testSqlserver2SqlserverAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink_autocreate_sqlserver2sqlserver_incr";
        Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlseverSource.getJdbcUrl(),
            "--source-user", sqlseverSource.getUsername(),
            "--source-password", sqlseverSource.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-staging-schema", sqlserver.getDatabaseName(),
            "--sink-auto-create", "true",
            "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertTrue(tableExists(sqlserverConn, sinkTable), "Sink table should exist after auto-create");
        assertEquals(TOTAL_SINK_ROWS, countRows(sqlserverConn, sinkTable));

        // Test merge by running again
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countRows(sqlserverConn, sinkTable), "Row count should remain same after merge");

        // Cleanup
        sqlserverConn.createStatement().execute("DROP TABLE " + sinkTable);
    }

    @Disabled("Parallel jobs with XML columns require per-job batch size configuration. " +
            "SQL Server 2019 requires fetch.size=1 for XML replication, but --fetch-size applies globally. " +
            "Need to implement per-job batch size control. Note: This limitation is SQL Server 2019 specific. " +
            "See: https://github.com/osalvador/ReplicaDB/issues/240")
    @Test
    void testSqlserver2SqlserverAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
        String sinkTable = "t_sink"; // Use existing table

        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect", sqlseverSource.getJdbcUrl(),
            "--source-user", sqlseverSource.getUsername(),
            "--source-password", sqlseverSource.getPassword(),
            "--sink-connect", sqlserver.getJdbcUrl(),
            "--sink-user", sqlserver.getUsername(),
            "--sink-password", sqlserver.getPassword(),
            "--sink-table", sinkTable,
            "--sink-auto-create", "true",
            "--source-columns", COLUMNS,
            "--sink-columns", COLUMNS,
            "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }
}
