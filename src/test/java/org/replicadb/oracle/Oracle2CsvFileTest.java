package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Oracle2CsvFileTest {
    private static final Logger LOG = LogManager.getLogger(Oracle2CsvFileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private Connection oracleConn;
    private static ReplicadbOracleContainer oracle;

    @BeforeAll
    static void setUp() {
        oracle = ReplicadbOracleContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
        // Reset static temp files map to avoid stale references between tests
        FileManager.setTempFilesPath(new HashMap<>());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Reset static temp files map
        FileManager.setTempFilesPath(new HashMap<>());
        this.oracleConn.close();
    }

    /**
     * Delete a sink file and its associated temp files
     */
    private void cleanupFile(String fileUriPath, String tempFilePrefix) {
        try {
            File sinkFile = new File(URI.create(fileUriPath));
            if (sinkFile.exists()) {
                Files.deleteIfExists(sinkFile.toPath());
                LOG.info("Deleted sink file: {}", sinkFile.getAbsolutePath());
            }
            
            // Clean up any temp files
            File tmpDir = new File("/tmp");
            File[] tempFiles = tmpDir.listFiles((dir, name) -> name.startsWith(tempFilePrefix));
            if (tempFiles != null) {
                for (File f : tempFiles) {
                    f.delete();
                    LOG.info("Deleted temp file: {}", f.getName());
                }
            }
        } catch (IOException e) {
            LOG.error("Error cleaning up files", e);
        }
    }

    /**
     * Count rows in a CSV file
     */
    private int countRows(String fileUriPath) throws IOException {
        Path path = Paths.get(URI.create(fileUriPath));
        try (var lines = Files.lines(path)) {
            int count = (int) lines.count();
            LOG.info("File total Rows: {}", count);
            return count;
        }
    }

    @Test
    void testOracleConnection() throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
        rs.next();
        String result = rs.getString(1);
        LOG.info("Oracle connection test: {}", result);
        assertTrue(result.contains("1"));
    }

    @Test
    void testOracleSourceRows() throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testOracle2CsvFileComplete() throws ParseException, IOException {
        // Use unique file name for this test
        String sinkFilePath = "file:///tmp/oracle2csv_complete_" + System.nanoTime() + ".csv";
        String tempFilePrefix = sinkFilePath.substring(sinkFilePath.lastIndexOf('/') + 1) + ".repdb.";
        
        // Clean before test
        cleanupFile(sinkFilePath, tempFilePrefix);
        FileManager.setTempFilesPath(new HashMap<>());
        
        // Verify temp files map is empty
        assertEquals(0, FileManager.getTempFilesPath().size(), "Temp files map should be empty before test");
        
        try {
            String[] args = {
                    "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                    "--source-connect", oracle.getJdbcUrl(),
                    "--source-user", oracle.getUsername(),
                    "--source-password", oracle.getPassword(),
                    "--sink-connect", sinkFilePath,
                    "--sink-file-format", FileFormats.CSV.getType()
            };
            ToolOptions options = new ToolOptions(args);
            assertEquals(0, ReplicaDB.processReplica(options));
            assertEquals(EXPECTED_ROWS, countRows(sinkFilePath));
        } finally {
            // Clean after test
            cleanupFile(sinkFilePath, tempFilePrefix);
        }
    }

    @Test
    void testOracle2CsvFileCompleteParallel() throws ParseException, IOException {
        // Use unique file name for this test
        String sinkFilePath = "file:///tmp/oracle2csv_parallel_" + System.nanoTime() + ".csv";
        String tempFilePrefix = sinkFilePath.substring(sinkFilePath.lastIndexOf('/') + 1) + ".repdb.";
        
        // Clean before test
        cleanupFile(sinkFilePath, tempFilePrefix);
        FileManager.setTempFilesPath(new HashMap<>());
        
        // Verify temp files map is empty
        assertEquals(0, FileManager.getTempFilesPath().size(), "Temp files map should be empty before test");
        
        try {
            String[] args = {
                    "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                    "--source-connect", oracle.getJdbcUrl(),
                    "--source-user", oracle.getUsername(),
                    "--source-password", oracle.getPassword(),
                    "--sink-connect", sinkFilePath,
                    "--sink-file-format", FileFormats.CSV.getType(),
                    "--jobs", "4"
            };
            ToolOptions options = new ToolOptions(args);
            assertEquals(0, ReplicaDB.processReplica(options));
            assertEquals(EXPECTED_ROWS, countRows(sinkFilePath));
        } finally {
            // Clean after test
            cleanupFile(sinkFilePath, tempFilePrefix);
        }
    }
}
