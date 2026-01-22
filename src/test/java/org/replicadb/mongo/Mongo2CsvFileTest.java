package org.replicadb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Mongo2CsvFileTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String SINK_COLLECTION = "t_sink";
    private static final String SOURCE_COLUMNS = "{_id:0,c_integer:1,c_smallint:1,c_bigint:1,c_numeric:1,c_decimal:1,c_real:1,c_double_precision:1,c_float:1,c_binary:1,c_binary_var:1,c_binary_lob:1,c_boolean:1,c_character:1,c_character_var:1,c_character_lob:1,c_national_character:1,c_national_character_var:1,c_date:1,c_timestamp_with_timezone:1}";

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
        FileManager.setTempFilesPath(new HashMap<>());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clean up connections
        mongoClient.getDatabase(mongoDatabaseName).getCollection(SINK_COLLECTION).deleteMany(new Document());
        this.mongoClient.close();
        FileManager.setTempFilesPath(new HashMap<>());
    }

    private void cleanupFile(String fileUriPath, String tempFilePrefix) {
        try {
            File sinkFile = new File(URI.create(fileUriPath));
            if (sinkFile.exists()) {
                Files.deleteIfExists(sinkFile.toPath());
            }
            File tmpDir = new File("/tmp");
            File[] tempFiles = tmpDir.listFiles((dir, name) -> name.startsWith(tempFilePrefix));
            if (tempFiles != null) {
                for (File f : tempFiles) {
                    f.delete();
                }
            }
        } catch (IOException e) {
            // Error cleaning up files
        }
    }

    private int countRows(String fileUriPath) throws IOException {
        Path path = Paths.get(URI.create(fileUriPath));
        try (var lines = Files.lines(path)) {
            return (int) lines.count();
        }
    }

    @Test
    void testMongodb2CsvFileComplete() throws ParseException, IOException {
        String sinkFilePath = "file:///tmp/mongo2csv_complete_" + System.nanoTime() + ".csv";
        String tempFilePrefix = sinkFilePath.substring(sinkFilePath.lastIndexOf('/') + 1) + ".repdb.";
        
        cleanupFile(sinkFilePath, tempFilePrefix);
        FileManager.setTempFilesPath(new HashMap<>());
        
        try {
            String[] args = {
                    "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                    "--source-connect", mongoContainer.getReplicaSetUrl(),
                    "--sink-connect", sinkFilePath,
                    "--sink-file-format", FileFormats.CSV.getType(),
                    "--source-columns", SOURCE_COLUMNS
            };
            ToolOptions options = new ToolOptions(args);
            assertEquals(0, ReplicaDB.processReplica(options));
            assertEquals(EXPECTED_ROWS, countRows(sinkFilePath));
        } finally {
            cleanupFile(sinkFilePath, tempFilePrefix);
        }
    }

    @Test
    void testMongodb2CsvFileCompleteParallel() throws ParseException, IOException {
        String sinkFilePath = "file:///tmp/mongo2csv_parallel_" + System.nanoTime() + ".csv";
        String tempFilePrefix = sinkFilePath.substring(sinkFilePath.lastIndexOf('/') + 1) + ".repdb.";
        
        cleanupFile(sinkFilePath, tempFilePrefix);
        FileManager.setTempFilesPath(new HashMap<>());
        
        try {
            String[] args = {
                    "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                    "--source-connect", mongoContainer.getReplicaSetUrl(),
                    "--sink-connect", sinkFilePath,
                    "--sink-file-format", FileFormats.CSV.getType(),
                    "--source-columns", SOURCE_COLUMNS,
                    "--jobs", "4"
            };
            ToolOptions options = new ToolOptions(args);
            assertEquals(0, ReplicaDB.processReplica(options));
            assertEquals(EXPECTED_ROWS, countRows(sinkFilePath));
        } finally {
            cleanupFile(sinkFilePath, tempFilePrefix);
        }
    }
}
