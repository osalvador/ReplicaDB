package org.replicadb.mariadb;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.MariaDBContainer;
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
class MariaDB2OrcFileTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private static final String SINK_FILE_PATH = "file:///tmp/mariadb2orc_sink.orc";
    private static final File sinkFile = new File("/tmp/mariadb2orc_sink.orc");

    private Connection mariadbConn;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        sinkFile.delete();
        this.mariadbConn.close();

        FileManager.setTempFilesPath(new HashMap<>());
    }

    public int countSinkRows() throws IOException {
        Path path = new Path(sinkFile.getPath());
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration(true)));
        int count = (int) reader.getNumberOfRows();
        return count;
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
    void testMariaDB2OrcFileComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
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
    void testMariaDB2OrcFileCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
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
