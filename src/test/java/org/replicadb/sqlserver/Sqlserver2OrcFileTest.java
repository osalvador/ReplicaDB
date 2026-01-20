package org.replicadb.sqlserver;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.MSSQLServerContainer;
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
class Sqlserver2OrcFileTest {
    private static final Logger LOG = LogManager.getLogger(Sqlserver2OrcFileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private static final String SINK_FILE_PATH = "file:///tmp/sqlserver2orc_sink.orc";
    private static final File sinkFile = new File("/tmp/sqlserver2orc_sink.orc");

    private Connection sqlserverConn;

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        sinkFile.delete();
        this.sqlserverConn.close();
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
    void testSqlserver2OrcFileComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
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
    void testSqlserver2OrcFileCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
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
