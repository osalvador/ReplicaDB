package org.replicadb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Mongo2MongoTest {
    private static final Logger LOG = LogManager.getLogger(Mongo2MongoTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String SINK_COLLECTION = "t_sink";

    private MongoClient mongoClient;
    private String mongoDatabaseName;

    @Rule
    public static ReplicadbMongodbContainer mongoContainer = ReplicadbMongodbContainer.getInstance();


    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.mongoClient = MongoClients.create(mongoContainer.getMongoConnectionString());
        this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        mongoClient.getDatabase(mongoDatabaseName).getCollection(SINK_COLLECTION).deleteMany(new Document());
        this.mongoClient.close();
    }


    public int countSinkRows() throws SQLException {
        // Count rows in sink table
        return (int) this.mongoClient.getDatabase(mongoDatabaseName).getCollection("t_sink").countDocuments();
    }

    @Test
    void testMongodb2MongoComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect",  mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMongodb2MongoCompleteQuery() throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect",  mongoContainer.getReplicaSetUrl(),
            "--sink-connect", mongoContainer.getReplicaSetUrl(),
            "--source-query", "[{$sort:{_id:1}}]"
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMongodb2MongoCompleteWhere() throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect",  mongoContainer.getReplicaSetUrl(),
            "--sink-connect", mongoContainer.getReplicaSetUrl(),
            "--source-where", "{c_integer:{$gt:-1}}"
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMongodb2MongoCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect",  mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(1, ReplicaDB.processReplica(options));
    }

    @Test
    void testMongodb2MongoIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect",  mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());

    }

    @Test
    void testMongodb2MongoCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect",  mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMongodb2MongoCompleteParallelQuery() throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect",  mongoContainer.getReplicaSetUrl(),
            "--sink-connect", mongoContainer.getReplicaSetUrl(),
            "--source-query", "[{$sort:{_id:1}}]",
            "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMongodb2MongoCompleteParallelWhere() throws ParseException, IOException, SQLException {
        String[] args = {
            "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
            "--source-connect",  mongoContainer.getReplicaSetUrl(),
            "--sink-connect", mongoContainer.getReplicaSetUrl(),
            "--source-where", "{c_integer:{$gt:-1}}",
            "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }

    @Test
    void testMongodb2MongoIncrementalParallelProjection() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect",  mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--source-columns", "{c_integer:1,c_string:1}",
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS,countSinkRows());
    }
}