package org.replicadb.postgres;

import org.apache.avro.Schema;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Postgres2CsvFileTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2CsvFileTest.class);
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String POSTGRES_SOURCE_FILE = "/postgres/pg-source.sql";
    private static final String USER_PASSWD_DB = "replicadb";
    private static final int TOTAL_SINK_ROWS = 4097;

    private static final String SINK_FILE_PATH = "file:///tmp/fileSink.csv";
    private static final String SINK_FILE_URI_PATH = "file:///tmp/fileSink.csv";

    private Connection postgresConn;

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:9.6")
            .withDatabaseName(USER_PASSWD_DB)
            .withUsername(USER_PASSWD_DB)
            .withPassword(USER_PASSWD_DB);

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        // Start the postgres container
        postgres.start();
        // Create tables
        /*Postgres*/
        Connection con = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        ScriptRunner runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + POSTGRES_SOURCE_FILE)));
        con.close();

    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
        LOG.info("Deleted file: {}",sinkFile.delete());
        this.postgresConn.close();
    }


    public int countSinkRows() throws IOException {
        Path path = Paths.get(URI.create(SINK_FILE_URI_PATH));
        int count = (int) Files.lines(path).count();
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
    void testFileComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType()
        };
        ToolOptions options = new ToolOptions(args);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }


    @Test
    void testFileCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testFileIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testFileIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", SINK_FILE_PATH,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

}