package org.replicadb.db2;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22CsvFileTest {
    private static final Logger LOG = LogManager.getLogger(DB22CsvFileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private static final String SINK_FILE_PATH = "file:///tmp/db22csv_sink.csv";
    private static final String SINK_FILE_URI_PATH = "file:///tmp/db22csv_sink.csv";

    private Connection db2Conn;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());

        File tmpDir = new File("/tmp");
        File[] tempFiles = tmpDir.listFiles((dir, name) -> name.contains("db22csv_sink.csv.repdb."));
        if (tempFiles != null) {
            for (File f : tempFiles) {
                LOG.info("Deleting leftover temp file: {}", f.delete() ? f.getName() : "FAILED: " + f.getName());
            }
        }

        FileManager.setTempFilesPath(new HashMap<>());

        File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
        if (sinkFile.exists()) {
            LOG.info("Deleting sink file before test: {}", sinkFile.delete());
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
        LOG.info("Deleted file: {}", sinkFile.delete());
        this.db2Conn.close();

        FileManager.setTempFilesPath(new HashMap<>());
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
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testDb2SourceRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testDb22CsvFileComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22CsvFileCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
