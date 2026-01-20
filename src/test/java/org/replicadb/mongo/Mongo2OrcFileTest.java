package org.replicadb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Mongo2OrcFileTest {
    private static final Logger LOG = LogManager.getLogger(Mongo2OrcFileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String SINK_COLLECTION = "t_sink";
    private static final String SOURCE_COLUMNS = "{_id:0,c_integer:1,c_smallint:1,c_bigint:1,c_numeric:1,c_decimal:1,c_real:1,c_double_precision:1,c_float:1,c_binary:1,c_binary_var:1,c_binary_lob:1,c_boolean:1,c_character:1,c_character_var:1,c_character_lob:1,c_national_character:1,c_national_character_var:1,c_date:1,c_timestamp_with_timezone:1}";
    private static final String SINK_FILE_PATH = "/tmp/mongo2orc_sink.orc";

    private MongoClient mongoClient;
    private String mongoDatabaseName;

    @Rule
    public static ReplicadbMongodbContainer mongoContainer = ReplicadbMongodbContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mongoClient = MongoClients.create(mongoContainer.getMongoConnectionString());
        this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
        // Delete sink file if exists
        File sinkFile = new File(SINK_FILE_PATH);
        if (sinkFile.exists()) {
            sinkFile.delete();
        }
        FileManager.setTempFilesPath(new HashMap<>());
    }

    @AfterEach
    void tearDown() throws SQLException, IOException {
        // Clean up connections
        mongoClient.getDatabase(mongoDatabaseName).getCollection(SINK_COLLECTION).deleteMany(new Document());
        this.mongoClient.close();
        // Delete sink file
        Files.deleteIfExists(Paths.get(SINK_FILE_PATH));
        // Clean up crc file
        Files.deleteIfExists(Paths.get("/tmp/." + new File(SINK_FILE_PATH).getName() + ".crc"));
        FileManager.setTempFilesPath(new HashMap<>());
    }

    public int countSinkRows() throws IOException {
        Path path = new Path(SINK_FILE_PATH);
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
        return (int) reader.getNumberOfRows();
    }

    @Test
    void testMongodb2OrcFileComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", "file://" + SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType(),
                "--source-columns", SOURCE_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("orc.compression", "snappy");
        options.setSinkConnectionParams(sinkConnectionParams);

        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMongodb2OrcFileCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", "file://" + SINK_FILE_PATH,
                "--sink-file-format", FileFormats.ORC.getType(),
                "--source-columns", SOURCE_COLUMNS,
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
