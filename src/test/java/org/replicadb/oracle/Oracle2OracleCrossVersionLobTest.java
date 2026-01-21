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
                LOG.info("Attempting to start Oracle 18c container...");
                sourceOracle = ReplicadbOracleContainer.getOracle18cInstance();
                LOG.info("Oracle 18c container started successfully");
            } catch (Exception e) {
                LOG.warn("Oracle 18c not available, attempting Oracle 21c. Reason: {}", 
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                sourceOracle = ReplicadbOracleContainer.getOracle21cInstance();
                LOG.info("Oracle 21c container started successfully");
            }
            
            LOG.info("Starting Oracle 23c container for sink...");
            sinkOracle = ReplicadbOracleContainer.getOracle23cInstance();
            containersAvailable = true;
            LOG.info("Oracle containers started successfully: source={}, sink={}", 
                sourceOracle.getOracleVersion(), sinkOracle.getOracleVersion());
        } catch (Exception e) {
            LOG.error("Could not start Oracle containers for cross-version testing", e);
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
        int expectedRows = 6; // Based on oracle-lob-source.sql
        LOG.info("=== Cross-version LOB Replication Test - Complete Mode ===");
        LOG.info("Expected rows in source: {}", expectedRows);
        LOG.info("Actual rows in source: {}", sourceRows);
        LOG.info("Source: Oracle {}, Sink: Oracle {}", 
            sourceOracle.getOracleVersion(), sinkOracle.getOracleVersion());
        
        if (sourceRows != expectedRows) {
            LOG.warn("Source row count mismatch! Expected: {}, Actual: {}. Test data may not have loaded correctly.", 
                expectedRows, sourceRows);
        }
        
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
        
        int sinkRows = countSinkRows();
        LOG.info("Replication completed. Rows in sink: {}", sinkRows);
        
        assertEquals(0, result, "Replication should succeed without ORA-64219");
        assertEquals(sourceRows, sinkRows, 
            String.format("All LOB rows should be replicated. Expected: %d, Actual: %d", sourceRows, sinkRows));
        
        // Verify LOB data integrity
        if (sourceRows > 0) {
            LOG.info("Verifying LOB data integrity...");
            verifyLobDataIntegrity();
            LOG.info("LOB data integrity verified successfully");
        }
    }

    @Test
    @DisplayName("Cross-version LOB replication - Incremental mode (ORA-64219 fix)")
    void testCrossVersionLobReplicationIncremental() throws ParseException, IOException, SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        int sourceRows = countSourceRows();
        int expectedRows = 6;
        LOG.info("=== Cross-version LOB Replication Test - Incremental Mode ===");
        LOG.info("Expected rows in source: {}", expectedRows);
        LOG.info("Actual rows in source: {}", sourceRows);
        
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
        
        int sinkRows = countSinkRows();
        LOG.info("Incremental replication completed. Rows in sink: {}", sinkRows);
        
        assertEquals(0, result, "Incremental replication should succeed without ORA-64219");
        assertEquals(sourceRows, sinkRows, 
            String.format("All LOB rows should be replicated. Expected: %d, Actual: %d", sourceRows, sinkRows));
    }

    @Test
    @DisplayName("Cross-version LOB replication with parallel jobs")
    void testCrossVersionLobReplicationParallel() throws ParseException, IOException, SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        int sourceRows = countSourceRows();
        int expectedRows = 6;
        LOG.info("=== Cross-version LOB Replication Test - Parallel Mode (2 jobs) ===");
        LOG.info("Expected rows in source: {}", expectedRows);
        LOG.info("Actual rows in source: {}", sourceRows);
        
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
        
        int sinkRows = countSinkRows();
        LOG.info("Parallel replication completed. Rows in sink: {}", sinkRows);
        
        assertEquals(0, result, "Parallel replication should succeed without ORA-64219");
        assertEquals(sourceRows, sinkRows, 
            String.format("All LOB rows should be replicated. Expected: %d, Actual: %d", sourceRows, sinkRows));
    }

    @Test
    @DisplayName("Cross-version LOB replication with large LOBs (200MB+)")
    void testCrossVersionLargeLobReplication() throws ParseException, IOException, SQLException {
        Assumptions.assumeTrue(containersAvailable, "Containers not available");
        
        LOG.info("=== Cross-version Large LOB Replication Test (200MB+) ===");
        LOG.info("Creating large LOBs (BLOB: 200MB, CLOB: 200MB, XMLTYPE: 200MB)...");
        
        long blobSize = 200 * 1024 * 1024; // 200MB
        long clobSize = 200 * 1024 * 1024; // 200MB (characters)
        long xmlSize = 200 * 1024 * 1024;   // 200MB
        
        // Create table with large LOB support
        createLargeLobTable(sourceConn, "t_large_lob_source");
        createLargeLobTable(sinkConn, "t_large_lob_sink");
        
        // Insert large LOB data
        LOG.info("Inserting large LOB data: BLOB={}MB, CLOB={}MB, XML={}MB", 
            blobSize / (1024*1024), clobSize / (1024*1024), xmlSize / (1024*1024));
        insertLargeLobData(sourceConn, blobSize, clobSize, xmlSize);
        
        int sourceRows = countRows(sourceConn, "t_large_lob_source");
        LOG.info("Large LOB source rows: {}", sourceRows);
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sourceOracle.getJdbcUrl(),
                "--source-user", sourceOracle.getUsername(),
                "--source-password", sourceOracle.getPassword(),
                "--source-table", "t_large_lob_source",
                "--sink-connect", sinkOracle.getJdbcUrl(),
                "--sink-user", sinkOracle.getUsername(),
                "--sink-password", sinkOracle.getPassword(),
                "--sink-table", "t_large_lob_sink",
                "--mode", ReplicationMode.COMPLETE.getModeText(),
                "--fetch-size", "10"  // Smaller fetch size for large LOBs
        };
        
        long startTime = System.currentTimeMillis();
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        long duration = (System.currentTimeMillis() - startTime) / 1000;
        
        int sinkRows = countRows(sinkConn, "t_large_lob_sink");
        LOG.info("Large LOB replication completed in {} seconds. Rows in sink: {}", duration, sinkRows);
        
        assertEquals(0, result, "Large LOB replication should succeed without ORA-64219");
        assertEquals(sourceRows, sinkRows, "All large LOB rows should be replicated");
        
        // Verify large LOB integrity
        verifyLargeLobIntegrity(blobSize, clobSize, xmlSize);
        LOG.info("Large LOB data integrity verified successfully");
        
        // Cleanup large LOB tables
        dropTable(sourceConn, "t_large_lob_source");
        dropTable(sinkConn, "t_large_lob_sink");
    }

    private void createLargeLobTable(Connection conn, String tableName) throws SQLException {
        String createSql = "CREATE TABLE " + tableName + " (" +
                "id NUMBER PRIMARY KEY, " +
                "blob_col BLOB, " +
                "clob_col CLOB, " +
                "xml_col XMLTYPE, " +
                "blob_size NUMBER, " +
                "clob_size NUMBER, " +
                "xml_size NUMBER, " +
                "created_at TIMESTAMP DEFAULT SYSTIMESTAMP)";
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSql);
            LOG.info("Created large LOB table: {}", tableName);
        } catch (SQLException e) {
            if (e.getErrorCode() == 955) { // ORA-00955: name already exists
                LOG.debug("Table {} already exists, truncating", tableName);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + tableName);
                }
            } else {
                throw e;
            }
        }
    }

    private void insertLargeLobData(Connection conn, long blobSize, long clobSize, long xmlSize) throws SQLException {
        String insertSql = "INSERT INTO t_large_lob_source " +
                "(id, blob_col, clob_col, xml_col, blob_size, clob_size, xml_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        conn.setAutoCommit(false);
        
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            // Create large BLOB (200MB)
            LOG.info("Creating 200MB BLOB...");
            byte[] blobChunk = new byte[1024 * 1024]; // 1MB chunks
            for (int i = 0; i < blobChunk.length; i++) {
                blobChunk[i] = (byte) ('A' + (i % 26));
            }
            
            Blob blob = conn.createBlob();
            long position = 1;
            for (long written = 0; written < blobSize; written += blobChunk.length) {
                int chunkSize = (int) Math.min(blobChunk.length, blobSize - written);
                blob.setBytes(position, blobChunk, 0, chunkSize);
                position += chunkSize;
                if (written % (50 * 1024 * 1024) == 0) {
                    LOG.debug("BLOB progress: {}MB / {}MB", written / (1024*1024), blobSize / (1024*1024));
                }
            }
            
            // Create large CLOB (200MB)
            LOG.info("Creating 200MB CLOB...");
            char[] clobChunk = new char[1024 * 1024]; // 1MB chunks
            for (int i = 0; i < clobChunk.length; i++) {
                clobChunk[i] = (char) ('A' + (i % 26));
            }
            
            Clob clob = conn.createClob();
            position = 1;
            for (long written = 0; written < clobSize; written += clobChunk.length) {
                int chunkSize = (int) Math.min(clobChunk.length, clobSize - written);
                clob.setString(position, new String(clobChunk, 0, chunkSize));
                position += chunkSize;
                if (written % (50 * 1024 * 1024) == 0) {
                    LOG.debug("CLOB progress: {}MB / {}MB", written / (1024*1024), clobSize / (1024*1024));
                }
            }
            
            // Create large XMLTYPE (50MB)
            LOG.info("Creating 50MB XMLTYPE...");
            StringBuilder xmlBuilder = new StringBuilder();
            xmlBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            xmlBuilder.append("<root>");
            long currentSize = xmlBuilder.length();
            int itemCount = 0;
            while (currentSize < xmlSize) {
                String item = "<item id=\"" + itemCount + "\"><data>" + 
                        "X".repeat(1000) + "</data></item>";
                xmlBuilder.append(item);
                currentSize += item.length();
                itemCount++;
                if (itemCount % 10000 == 0) {
                    LOG.debug("XML progress: {}MB / {}MB", currentSize / (1024*1024), xmlSize / (1024*1024));
                }
            }
            xmlBuilder.append("</root>");
            
            SQLXML sqlxml = conn.createSQLXML();
            sqlxml.setString(xmlBuilder.toString());
            
            // Insert the large LOBs
            pstmt.setInt(1, 1);
            pstmt.setBlob(2, blob);
            pstmt.setClob(3, clob);
            pstmt.setSQLXML(4, sqlxml);
            pstmt.setLong(5, blobSize);
            pstmt.setLong(6, clobSize);
            pstmt.setLong(7, xmlBuilder.length());
            pstmt.executeUpdate();
            
            conn.commit();
            LOG.info("Large LOB data inserted successfully");
            
            // Free resources
            blob.free();
            clob.free();
            sqlxml.free();
        } catch (SQLException e) {
            conn.rollback();
            LOG.error("Failed to insert large LOB data", e);
            throw e;
        }
    }

    private void verifyLargeLobIntegrity(long expectedBlobSize, long expectedClobSize, long expectedXmlSize) throws SQLException {
        Statement sourceStmt = sourceConn.createStatement();
        Statement sinkStmt = sinkConn.createStatement();
        
        ResultSet sourceRs = sourceStmt.executeQuery(
            "SELECT id, " +
            "NVL(DBMS_LOB.GETLENGTH(blob_col), 0) as blob_len, " +
            "NVL(DBMS_LOB.GETLENGTH(clob_col), 0) as clob_len, " +
            "NVL(LENGTH(XMLSERIALIZE(CONTENT xml_col AS CLOB)), 0) as xml_len " +
            "FROM t_large_lob_source ORDER BY id");
        ResultSet sinkRs = sinkStmt.executeQuery(
            "SELECT id, " +
            "NVL(DBMS_LOB.GETLENGTH(blob_col), 0) as blob_len, " +
            "NVL(DBMS_LOB.GETLENGTH(clob_col), 0) as clob_len, " +
            "NVL(LENGTH(XMLSERIALIZE(CONTENT xml_col AS CLOB)), 0) as xml_len " +
            "FROM t_large_lob_sink ORDER BY id");
        
        while (sourceRs.next() && sinkRs.next()) {
            int id = sourceRs.getInt("id");
            long sourceBlobLen = sourceRs.getLong("blob_len");
            long sinkBlobLen = sinkRs.getLong("blob_len");
            long sourceClobLen = sourceRs.getLong("clob_len");
            long sinkClobLen = sinkRs.getLong("clob_len");
            long sourceXmlLen = sourceRs.getLong("xml_len");
            long sinkXmlLen = sinkRs.getLong("xml_len");
            
            assertEquals(sourceBlobLen, sinkBlobLen, 
                String.format("BLOB length mismatch for id=%d: expected=%d, actual=%d", 
                    id, sourceBlobLen, sinkBlobLen));
            assertEquals(sourceClobLen, sinkClobLen, 
                String.format("CLOB length mismatch for id=%d: expected=%d, actual=%d", 
                    id, sourceClobLen, sinkClobLen));
            assertEquals(sourceXmlLen, sinkXmlLen, 
                String.format("XMLTYPE length mismatch for id=%d: expected=%d, actual=%d", 
                    id, sourceXmlLen, sinkXmlLen));
            
            LOG.info("Verified large LOB integrity for id={}: BLOB={}MB, CLOB={}MB, XML={}MB", 
                id, sourceBlobLen / (1024*1024), sourceClobLen / (1024*1024), sourceXmlLen / (1024*1024));
        }
    }

    private int countRows(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + tableName)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private void dropTable(Connection conn, String tableName) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE " + tableName);
            LOG.debug("Dropped table: {}", tableName);
        } catch (SQLException e) {
            LOG.debug("Could not drop table {}: {}", tableName, e.getMessage());
        }
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
