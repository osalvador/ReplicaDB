package org.replicadb.mongo;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbLocalStackContainer;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.replicadb.manager.file.FileFormats;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Mongo2S3FileTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String SINK_COLLECTION = "t_sink";
    private static final String SOURCE_COLUMNS = "{_id:0,c_integer:1,c_smallint:1,c_bigint:1,c_numeric:1,c_decimal:1,c_real:1,c_double_precision:1,c_float:1,c_binary:1,c_binary_var:1,c_binary_lob:1,c_boolean:1,c_character:1,c_character_var:1,c_character_lob:1,c_national_character:1,c_national_character_var:1,c_date:1,c_timestamp_with_timezone:1}";

    private MongoClient mongoClient;
    private String mongoDatabaseName;
    private AmazonS3 s3Client;

    @Rule
    public static ReplicadbMongodbContainer mongoContainer = ReplicadbMongodbContainer.getInstance();

    private static ReplicadbLocalStackContainer localstack;

    @BeforeAll
    static void setUp() {
        localstack = ReplicadbLocalStackContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.mongoClient = MongoClients.create(mongoContainer.getMongoConnectionString());
        this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
        this.s3Client = localstack.createS3Client();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clean up connections
        mongoClient.getDatabase(mongoDatabaseName).getCollection(SINK_COLLECTION).deleteMany(new Document());
        this.mongoClient.close();
    }

    @Test
    void testMongodb2S3FileComplete() throws ParseException, IOException {
        String s3Url = localstack.getS3ConnectionString() + "/mongo2s3_test.csv";
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", s3Url,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--source-columns", SOURCE_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        
        // Set S3 connection params
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("accessKey", localstack.getAccessKey());
        sinkConnectionParams.setProperty("secretKey", localstack.getSecretKey());
        sinkConnectionParams.setProperty("secure-connection", "false");
        options.setSinkConnectionParams(sinkConnectionParams);
        
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify file was created in S3
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        assertTrue(objects.size() > 0, "S3 bucket should contain at least one file");
    }

    @Test
    void testMongodb2S3FileCompleteParallel() throws ParseException, IOException {
        String s3Url = localstack.getS3ConnectionString() + "/mongo2s3_parallel_test.csv";
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", s3Url,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--source-columns", SOURCE_COLUMNS,
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        
        // Set S3 connection params
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("accessKey", localstack.getAccessKey());
        sinkConnectionParams.setProperty("secretKey", localstack.getSecretKey());
        sinkConnectionParams.setProperty("secure-connection", "false");
        options.setSinkConnectionParams(sinkConnectionParams);
        
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify file was created in S3
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        assertTrue(objects.size() > 0, "S3 bucket should contain at least one file");
    }
}
