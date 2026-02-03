package org.replicadb.sqlserver;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Sqlserver2PostgresTest {
    private static final Logger LOG = LogManager.getLogger(Sqlserver2PostgresTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    private static final String IMAGE_SOURCE_FILE = "/sqlserver/sqlserver-image-source.sql";
    private static final String IMAGE_SINK_FILE = "/sinks/pg-image-sink.sql";

    private Connection sqlserverConn;
    private Connection postgresConn;

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();
    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
        // Clean up IMAGE test tables if they exist
        postgresConn.createStatement().execute("DROP TABLE IF EXISTS t_image_sink");
        this.sqlserverConn.close();
        this.postgresConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
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
    void testSqlserverSourceRows() throws SQLException {
        Statement stmt = sqlserverConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testSqlserver2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2PostgresIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-staging-schema", "public",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    /**
     * Helper method to set up IMAGE test tables in SQL Server and PostgreSQL.
     * Creates t_image_source with IMAGE column and t_image_sink with BYTEA column.
     */
    private void setupImageTables() throws SQLException, IOException {
        ScriptRunner sqlserverRunner = new ScriptRunner(sqlserverConn, false, true);
        ScriptRunner postgresRunner = new ScriptRunner(postgresConn, false, true);

        try {
            sqlserverRunner.runScript(new BufferedReader(
                new FileReader(RESOURCE_DIR + IMAGE_SOURCE_FILE)));
            LOG.info("IMAGE source table created");
        } catch (SQLException e) {
            LOG.error("Failed to create IMAGE source table", e);
            throw e;
        }

        try {
            postgresRunner.runScript(new BufferedReader(
                new FileReader(RESOURCE_DIR + IMAGE_SINK_FILE)));
            LOG.info("IMAGE sink table created");
        } catch (SQLException e) {
            LOG.error("Failed to create IMAGE sink table", e);
            throw e;
        }
    }

    /**
     * Test IMAGE replication WITHOUT cast - validates Issue #202 fix.
     * Expected behavior: Binary data transferred correctly using PostgreSQL binary COPY format.
     * Binary lengths should match exactly (no hex encoding).
     */
    @Test
    void testImageReplicationWithoutCast() throws ParseException, IOException, SQLException {
        setupImageTables();

        String[] args = {
                "--mode", ReplicationMode.COMPLETE.getModeText(),
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--source-table", "t_image_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", "t_image_sink"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        // Verify row count matches
        Statement postgresStmt = postgresConn.createStatement();
        ResultSet countRs = postgresStmt.executeQuery("SELECT count(*) FROM t_image_sink");
        countRs.next();
        assertEquals(5, countRs.getInt(1), "Expected 5 rows replicated");

        // Verify binary data is doubled in size (hex encoding bug)
        Statement sqlserverStmt = sqlserverConn.createStatement();
        ResultSet sourceRs = sqlserverStmt.executeQuery(
            "SELECT id, DATALENGTH(image_col) as len FROM t_image_source WHERE image_col IS NOT NULL ORDER BY id");

        ResultSet sinkRs = postgresStmt.executeQuery(
            "SELECT id, LENGTH(image_col) as len FROM t_image_sink WHERE image_col IS NOT NULL ORDER BY id");

        while (sourceRs.next() && sinkRs.next()) {
            int id = sourceRs.getInt("id");
            long sourceLen = sourceRs.getLong("len");
            long sinkLen = sinkRs.getLong("len");

            LOG.info("Row {}: Source length = {}, Sink length = {}", id, sourceLen, sinkLen);

            // Assert that sink length matches source length exactly (binary COPY format fix)
            assertEquals(sourceLen, sinkLen,
                "Binary length should match for id=" + id +
                " (source=" + sourceLen + ", sink=" + sinkLen + ")");
        }
        
        LOG.info("✓ IMAGE replication validated: Binary data transferred correctly without CAST");
    }

    /**
     * Test IMAGE replication WITH cast workaround - validates Issue #202 fix.
     * Expected behavior: Binary data lengths should match exactly (no hex encoding).
     * Uses source-query instead of source-columns to apply CAST on SQL Server side.
     * Note: SQL Server doesn't support parallel replication with custom queries, so jobs=1.
     * 
     * This test validates that the PostgreSQL binary COPY format fix correctly handles
     * binary data from SQL Server, preserving byte-for-byte fidelity.
     */
    @Test
    void testImageReplicationWithCastWorkaround() throws ParseException, IOException, SQLException {
        setupImageTables();

        String[] args = {
                "--mode", ReplicationMode.COMPLETE.getModeText(),
                "--jobs", "1",
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--source-query", "SELECT id, CAST(image_col AS varbinary(max)) as image_col, image_size, description FROM t_image_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", "t_image_sink",
                "--sink-columns", "id,image_col,image_size,description"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        // Verify row count matches
        Statement postgresStmt = postgresConn.createStatement();
        ResultSet countRs = postgresStmt.executeQuery("SELECT count(*) FROM t_image_sink");
        countRs.next();
        assertEquals(5, countRs.getInt(1), "Expected 5 rows replicated");

        // Verify binary data lengths - measure the CAST version to match what's extracted
        Statement sqlserverStmt = sqlserverConn.createStatement();
        ResultSet sourceRs = sqlserverStmt.executeQuery(
            "SELECT id, DATALENGTH(CAST(image_col AS varbinary(max))) as len FROM t_image_source WHERE image_col IS NOT NULL ORDER BY id");

        ResultSet sinkRs = postgresStmt.executeQuery(
            "SELECT id, LENGTH(image_col) as len FROM t_image_sink WHERE image_col IS NOT NULL ORDER BY id");

        while (sourceRs.next() && sinkRs.next()) {
            int id = sourceRs.getInt("id");
            long sourceLen = sourceRs.getLong("len");
            long sinkLen = sinkRs.getLong("len");

            LOG.info("Row {}: Source length = {}, Sink length = {}", id, sourceLen, sinkLen);

            // This assertion will fail until ReplicaDB handles binary data correctly
            // Expected: sourceLen == sinkLen
            // Actual: sinkLen == sourceLen * 2 (hex encoding bug)
            assertEquals(sourceLen, sinkLen,
                "Binary length mismatch for id=" + id + " (source=" + sourceLen + ", sink=" + sinkLen + ")");
        }

        LOG.info("✓ IMAGE cast workaround validated: Binary data transferred correctly");
    }
}
