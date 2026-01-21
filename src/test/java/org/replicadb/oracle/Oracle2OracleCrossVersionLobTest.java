package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Oracle to Oracle cross-version LOB replication.
 * Verifies fix for ORA-64219: invalid LOB locator encountered.
 * 
 * This test uses different Oracle versions for source and sink to ensure
 * LOB streaming works correctly across database versions.
 */
@Testcontainers
class Oracle2OracleCrossVersionLobTest {
    private static final Logger LOG = LogManager.getLogger(Oracle2OracleCrossVersionLobTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String LOB_SOURCE_FILE = "/oracle/oracle-lob-source.sql";
    private static final String LOB_SINK_FILE = "/sinks/oracle-lob-sink.sql";

    private Connection sourceConn;
    private Connection sinkConn;
    private static ReplicadbOracleContainer sourceOracle;
    private static ReplicadbOracleContainer sinkOracle;
    private static boolean containersAvailable = false;

    @BeforeAll
    static void setUp() {
        try {
            // Use Oracle 18c as source (older version) and Oracle 23c as sink (newer version)
            // This simulates the cross-version scenario reported in issue #213
            LOG.info("Starting Oracle containers for cross-version LOB testing...");
            
            // Try to start 18c first, fall back to 21c if not available (ARM compatibility)
            try {
                sourceOracle = ReplicadbOracleContainer.getOracle18cInstance();
            } catch (Exception e) {
                LOG.warn("Oracle 18c not available (possibly ARM architecture), using 21c: {}", e.getMessage());
                sourceOracle = ReplicadbOracleContainer.getOracle21cInstance();
            }
            
            sinkOracle = ReplicadbOracleContainer.getOracle23cInstance();
            containersAvailable = true;
            LOG.info("Oracle containers started: source={}, sink={}", 
                sourceOracle.getOracleVersion(), sinkOracle.getOracleVersion());
        } catch (Exception e) {
            LOG.warn("Could not start Oracle containers for cross-version testing: {}", e.getMessage());
            containersAvailable = false;
        }
    }

    @BeforeEach
    void before() throws SQLException, IOException {
        Assumptions.assumeTrue(containersAvailable, "Oracle containers not available");
        
        this.sourceConn = DriverManager.getConnection(
            sourceOracle.getJdbcUrl(), sourceOracle.getUsername(), sourceOracle.getPassword());
        this.sinkConn = DriverManager.getConnection(
            sinkOracle.getJdbcUrl(), sinkOracle.getUsername(), sinkOracle.getPassword());
        
        // Setup LOB test tables
        setupLobTables();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (sinkConn != null && !sinkConn.isClosed()) {
            try {
                sinkConn.createStatement().execute("TRUNCATE TABLE t_lob_sink");
            } catch (SQLException e) {
                LOG.warn("Could not truncate sink table: {}", e.getMessage());
            }
            sinkConn.close();
        }
        if (sourceConn != null && !sourceConn.isClosed()) {
            sourceConn.close();
        }
    }

    private void setupLobTables() throws SQLException, IOException {
        ScriptRunner sourceRunner = new ScriptRunner(sourceConn, false, true);
        ScriptRunner sinkRunner = new ScriptRunner(sinkConn, false, true);
        
        // Create and populate source LOB table
        try {
            sourceRunner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + LOB_SOURCE_FILE)));
            LOG.info("LOB source table created and populated");
        } catch (Exception e) {
            LOG.debug("LOB source table may already exist: {}", e.getMessage());
        }
        
        // Create sink LOB table
        try {
            sinkRunner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + LOB_SINK_FILE)));
            LOG.info("LOB sink table created");
        } catch (Exception e) {
            LOG.debug("LOB sink table may already exist: {}", e.getMessage());
        }
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = sinkConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_lob_sink");
        rs.next();
        return rs.getInt(1);
    }

    public int countSourceRows() throws SQLException {
        Statement stmt = sourceConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_lob_source");
        rs.next();
        return rs.getInt(1);
    }

    @Test
    @DisplayName("Verify cross-version Oracle containers are running")
    void testCrossVersionContainersRunning() throws SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        Statement sourceStmt = sourceConn.createStatement();
        ResultSet sourceRs = sourceStmt.executeQuery("SELECT 1 FROM DUAL");
        assertTrue(sourceRs.next());
        
        Statement sinkStmt = sinkConn.createStatement();
        ResultSet sinkRs = sinkStmt.executeQuery("SELECT 1 FROM DUAL");
        assertTrue(sinkRs.next());
        
        LOG.info("Cross-version test: Source Oracle {} -> Sink Oracle {}", 
            sourceOracle.getOracleVersion(), sinkOracle.getOracleVersion());
    }

    @Test
    @DisplayName("Cross-version LOB replication - Complete mode (ORA-64219 fix)")
    void testCrossVersionLobReplicationComplete() throws ParseException, IOException, SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        int sourceRows = countSourceRows();
        LOG.info("Source LOB rows: {}", sourceRows);
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sourceOracle.getJdbcUrl(),
                "--source-user", sourceOracle.getUsername(),
                "--source-password", sourceOracle.getPassword(),
                "--source-table", "t_lob_source",
                "--sink-connect", sinkOracle.getJdbcUrl(),
                "--sink-user", sinkOracle.getUsername(),
                "--sink-password", sinkOracle.getPassword(),
                "--sink-table", "t_lob_sink",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        
        assertEquals(0, result, "Replication should succeed without ORA-64219");
        assertEquals(sourceRows, countSinkRows(), "All LOB rows should be replicated");
        
        // Verify LOB data integrity
        verifyLobDataIntegrity();
    }

    @Test
    @DisplayName("Cross-version LOB replication - Incremental mode (ORA-64219 fix)")
    void testCrossVersionLobReplicationIncremental() throws ParseException, IOException, SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        int sourceRows = countSourceRows();
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sourceOracle.getJdbcUrl(),
                "--source-user", sourceOracle.getUsername(),
                "--source-password", sourceOracle.getPassword(),
                "--source-table", "t_lob_source",
                "--sink-connect", sinkOracle.getJdbcUrl(),
                "--sink-user", sinkOracle.getUsername(),
                "--sink-password", sinkOracle.getPassword(),
                "--sink-table", "t_lob_sink",
                "--sink-staging-schema", sinkOracle.getUsername(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        
        assertEquals(0, result, "Incremental replication should succeed without ORA-64219");
        assertEquals(sourceRows, countSinkRows(), "All LOB rows should be replicated");
    }

    @Test
    @DisplayName("Cross-version LOB replication with parallel jobs")
    void testCrossVersionLobReplicationParallel() throws ParseException, IOException, SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        int sourceRows = countSourceRows();
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sourceOracle.getJdbcUrl(),
                "--source-user", sourceOracle.getUsername(),
                "--source-password", sourceOracle.getPassword(),
                "--source-table", "t_lob_source",
                "--sink-connect", sinkOracle.getJdbcUrl(),
                "--sink-user", sinkOracle.getUsername(),
                "--sink-password", sinkOracle.getPassword(),
                "--sink-table", "t_lob_sink",
                "--jobs", "2"
        };
        
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        
        assertEquals(0, result, "Parallel replication should succeed without ORA-64219");
        assertEquals(sourceRows, countSinkRows(), "All LOB rows should be replicated");
    }

    private void verifyLobDataIntegrity() throws SQLException {
        // Verify BLOB sizes match
        Statement sourceStmt = sourceConn.createStatement();
        Statement sinkStmt = sinkConn.createStatement();
        
        ResultSet sourceRs = sourceStmt.executeQuery(
            "SELECT id, NVL(DBMS_LOB.GETLENGTH(blob_col), 0) as blob_len, " +
            "NVL(DBMS_LOB.GETLENGTH(clob_col), 0) as clob_len FROM t_lob_source ORDER BY id");
        ResultSet sinkRs = sinkStmt.executeQuery(
            "SELECT id, NVL(DBMS_LOB.GETLENGTH(blob_col), 0) as blob_len, " +
            "NVL(DBMS_LOB.GETLENGTH(clob_col), 0) as clob_len FROM t_lob_sink ORDER BY id");
        
        while (sourceRs.next() && sinkRs.next()) {
            int id = sourceRs.getInt("id");
            long sourceBlobLen = sourceRs.getLong("blob_len");
            long sinkBlobLen = sinkRs.getLong("blob_len");
            long sourceClobLen = sourceRs.getLong("clob_len");
            long sinkClobLen = sinkRs.getLong("clob_len");
            
            assertEquals(sourceBlobLen, sinkBlobLen, 
                "BLOB length mismatch for id=" + id);
            assertEquals(sourceClobLen, sinkClobLen, 
                "CLOB length mismatch for id=" + id);
            
            LOG.debug("Verified LOB integrity for id={}: BLOB={} bytes, CLOB={} chars", 
                id, sourceBlobLen, sourceClobLen);
        }
    }
}
