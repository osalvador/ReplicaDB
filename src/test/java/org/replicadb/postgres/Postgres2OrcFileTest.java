package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Postgres2OrcFileTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2OrcFileTest.class);
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;

    private static final String SINK_FILE_PATH = "file:///tmp/fileSink.orc";
    private static final File sinkFile = new File("/tmp/fileSink.orc");

    private Connection postgresConn;

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();

    @BeforeAll
    static void setUp () {
        // Start the container is not necessary
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        sinkFile.delete();
        this.postgresConn.close();

        // Clean the static temFiles HasMap.
        FileManager.setTempFilesPath(new HashMap<>());
    }


    public int countSinkRows() throws IOException {
        Path path = new Path(sinkFile.getPath());
        Reader reader = OrcFile.createReader(path,OrcFile.readerOptions(new Configuration(true)));
        int count = (int) reader.getNumberOfRows();
        LOG.info("File total Rows:{}", count);
        return count;
    }


    @Test
    void testPostgresConnection() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) from t_source");
        rs.next();
        int totalRows = rs.getInt(1);
        assertEquals(TOTAL_SINK_ROWS, totalRows);
    }

    @Test
    void testPostgres2OrcFileComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType()
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "snappy"); //NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD)
        options.setSinkConnectionParams(sinkConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }


    @Test
    void testPostgres2OrcFileCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "ZSTD"); //NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD)
        options.setSinkConnectionParams(sinkConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2OrcFileIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "LZ0"); //NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD)
        options.setSinkConnectionParams(sinkConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2OrcFileIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "LZ4"); //NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD)
        options.setSinkConnectionParams(sinkConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

}