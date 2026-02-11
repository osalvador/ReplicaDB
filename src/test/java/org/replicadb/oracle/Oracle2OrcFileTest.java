package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Oracle2OrcFileTest {
    private static final Logger LOG = LogManager.getLogger(Oracle2OrcFileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private static final String SINK_FILE_PATH = "file:///tmp/oracle2orc_sink.orc";
    private static final File sinkFile = new File("/tmp/oracle2orc_sink.orc");

    private Connection oracleConn;
    private static ReplicadbOracleContainer oracle;

    @BeforeAll
    static void setUp() {
        oracle = ReplicadbOracleContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        sinkFile.delete();
        this.oracleConn.close();

        FileManager.setTempFilesPath(new HashMap<>());
    }

    public int countSinkRows() throws IOException {
        Path path = new Path(sinkFile.getPath());
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration(true)));
        int count = (int) reader.getNumberOfRows();
        LOG.info("File total Rows: {}", count);
        return count;
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
    void testOracle2OrcFileComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType()
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "snappy");
        options.setSinkConnectionParams(sinkConnectionParams);

        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testOracle2OrcFileCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "snappy");
        options.setSinkConnectionParams(sinkConnectionParams);

        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
