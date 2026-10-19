package org.replicadb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Mongo2MongoTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String SINK_COLLECTION = "t_sink";
    private static final String SOURCE_COLLECTION = "t_source";
    private static final String UNIQUE_KEY_SINK_COLLECTION = "t_sink_ci";
    private static final String ID_KEY_SINK_COLLECTION = "t_sink_id";
    private static final String UNIQUE_KEY_FIELD = "c_integer";
    private static final int CHANGED_BELOW = 10;
    private static final String CHANGED_VALUE = "changed-by-test";

    private MongoClient mongoClient;
    private String mongoDatabaseName;

    @Container
    public static final ReplicadbMongodbContainer mongoContainer = ReplicadbMongodbContainer.getInstance();


    @BeforeAll
    static void setUp(){
        if (!mongoContainer.isRunning()) {
            mongoContainer.start();
        }
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
        mongoClient.getDatabase(mongoDatabaseName).getCollection(UNIQUE_KEY_SINK_COLLECTION).drop();
        mongoClient.getDatabase(mongoDatabaseName).getCollection(ID_KEY_SINK_COLLECTION).drop();
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
        mongoClient.getDatabase(mongoDatabaseName).getCollection(UNIQUE_KEY_SINK_COLLECTION)
                .createIndex(Indexes.ascending(UNIQUE_KEY_FIELD), new IndexOptions().unique(true));
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect",  mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-table", UNIQUE_KEY_SINK_COLLECTION,
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--source-columns", "{c_integer:1,c_string:1}",
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countCollection(UNIQUE_KEY_SINK_COLLECTION));
    }

    @Test
    void testMongodb2MongoIncrementalRerunIdMergeKey() throws ParseException, IOException, SQLException {
        assertEquals(0, ReplicaDB.processReplica(incrementalOptions(ID_KEY_SINK_COLLECTION)));
        assertEquals(EXPECTED_ROWS, countCollection(ID_KEY_SINK_COLLECTION));
        assertEquals(0, ReplicaDB.processReplica(incrementalOptions(ID_KEY_SINK_COLLECTION)));
        assertEquals(EXPECTED_ROWS, countCollection(ID_KEY_SINK_COLLECTION));
    }

    @Test
    void testMongodb2MongoIncrementalChangedDocumentsIdMergeKey() throws ParseException, IOException, SQLException {
        assertEquals(0, ReplicaDB.processReplica(incrementalOptions(ID_KEY_SINK_COLLECTION)));
        MongoCollection<Document> source = mongoClient.getDatabase(mongoDatabaseName).getCollection(SOURCE_COLLECTION);
        MongoCollection<Document> sink = mongoClient.getDatabase(mongoDatabaseName).getCollection(ID_KEY_SINK_COLLECTION);
        List<Document> originals = source.find(Filters.lt(UNIQUE_KEY_FIELD, CHANGED_BELOW)).into(new ArrayList<>());
        try {
            source.updateMany(Filters.lt(UNIQUE_KEY_FIELD, CHANGED_BELOW), Updates.set("c_string", CHANGED_VALUE));
            assertEquals(0, ReplicaDB.processReplica(incrementalOptions(ID_KEY_SINK_COLLECTION)));
            assertEquals(EXPECTED_ROWS, countCollection(ID_KEY_SINK_COLLECTION));
            assertEquals(originals.size(), sink.countDocuments(Filters.eq("c_string", CHANGED_VALUE)));
        } finally {
            for (Document original : originals) {
                source.updateOne(Filters.eq("_id", original.get("_id")), Updates.set("c_string", original.get("c_string")));
            }
        }
    }

    @Test
    void testMongodb2MongoIncrementalIdMergeKeyWithoutSourceIdFails() throws ParseException, IOException, SQLException {
        assertEquals(0, ReplicaDB.processReplica(incrementalOptions(ID_KEY_SINK_COLLECTION)));
        assertEquals(EXPECTED_ROWS, countCollection(ID_KEY_SINK_COLLECTION));
        ToolOptions options = incrementalOptions(ID_KEY_SINK_COLLECTION, "--source-columns", "{_id:0,c_integer:1,c_string:1}");
        assertEquals(1, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countCollection(ID_KEY_SINK_COLLECTION));
    }

    @Test
    void testMongodb2MongoIncrementalRerunNonIdMergeKey() throws ParseException, IOException, SQLException {
        mongoClient.getDatabase(mongoDatabaseName).getCollection(UNIQUE_KEY_SINK_COLLECTION)
                .createIndex(Indexes.ascending(UNIQUE_KEY_FIELD), new IndexOptions().unique(true));
        assertEquals(0, ReplicaDB.processReplica(incrementalOptions(UNIQUE_KEY_SINK_COLLECTION)));
        assertEquals(EXPECTED_ROWS, countCollection(UNIQUE_KEY_SINK_COLLECTION));
        assertEquals(0, ReplicaDB.processReplica(incrementalOptions(UNIQUE_KEY_SINK_COLLECTION)));
        assertEquals(EXPECTED_ROWS, countCollection(UNIQUE_KEY_SINK_COLLECTION));
    }

    private ToolOptions incrementalOptions(String sinkCollection, String... extra) throws ParseException, IOException {
        List<String> args = new ArrayList<>(List.of("--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--sink-table", sinkCollection));
        args.addAll(List.of(extra));
        return new ToolOptions(args.toArray(new String[0]));
    }

    private int countCollection(String collectionName) {
        return (int) mongoClient.getDatabase(mongoDatabaseName).getCollection(collectionName).countDocuments();
    }
}
