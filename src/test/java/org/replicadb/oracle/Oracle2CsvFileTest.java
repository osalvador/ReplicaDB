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

    private static final String SINK_FILE_PATH = "file:///tmp/oracle2csv_sink.csv";
    private static final String SINK_FILE_URI_PATH = "file:///tmp/oracle2csv_sink.csv";

    private Connection oracleConn;
    private static ReplicadbOracleContainer oracle;

    @BeforeAll
    static void setUp() {
        oracle = ReplicadbOracleContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
        // Reset static temp files map FIRST to avoid stale references
        FileManager.setTempFilesPath(new HashMap<>());
        
        // Ensure file and temp files are deleted before test
        File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
        if (sinkFile.exists()) {
            LOG.info("Deleting existing sink file before test: {}", sinkFile.delete());
        }
        // Clean up any temp files from previous runs
        File tmpDir = new File("/tmp");
        File[] tempFiles = tmpDir.listFiles((dir, name) -> name.startsWith("oracle2csv_sink.csv.repdb."));
        if (tempFiles != null) {
            for (File f : tempFiles) {
                LOG.info("Deleting temp file: {} - {}", f.getName(), f.delete());
            }
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Reset static temp files map
        FileManager.setTempFilesPath(new HashMap<>());
        
        // Delete sink file
        File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
        LOG.info("Deleted sink file: {}", sinkFile.delete());
        
        // Clean up any temp files
        File tmpDir = new File("/tmp");
        File[] tempFiles = tmpDir.listFiles((dir, name) -> name.startsWith("oracle2csv_sink.csv.repdb."));
        if (tempFiles != null) {
            for (File f : tempFiles) {
                LOG.info("Deleting temp file in tearDown: {} - {}", f.getName(), f.delete());
            }
        }
        
        this.oracleConn.close();
    }

    public int countSinkRows() throws IOException {
        Path path = Paths.get(URI.create(SINK_FILE_URI_PATH));
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
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testOracle2CsvFileCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
