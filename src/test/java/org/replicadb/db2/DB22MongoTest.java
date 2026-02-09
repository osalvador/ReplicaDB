package org.replicadb.db2;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22MongoTest {
    private static final Logger LOG = LogManager.getLogger(DB22MongoTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
        private static final String SOURCE_QUERY = "SELECT "
            + "C_INTEGER AS c_integer,"
            + "C_SMALLINT AS c_smallint,"
            + "C_BIGINT AS c_bigint,"
            + "C_NUMERIC AS c_numeric,"
            + "C_DECIMAL AS c_decimal,"
            + "C_REAL AS c_real,"
            + "C_DOUBLE_PRECISION AS c_double_precision,"
            + "C_FLOAT AS c_float,"
            + "C_BINARY AS c_binary,"
            + "C_BINARY_VAR AS c_binary_var,"
            + "C_BINARY_LOB AS c_binary_lob,"
            + "C_BOOLEAN AS c_boolean,"
            + "C_CHARACTER AS c_character,"
            + "C_CHARACTER_VAR AS c_character_var,"
            + "C_CHARACTER_LOB AS c_character_lob,"
            + "C_NATIONAL_CHARACTER AS c_national_character,"
            + "C_NATIONAL_CHARACTER_VAR AS c_national_character_var,"
            + "C_DATE AS c_date,"
            + "C_TIMESTAMP_WITHOUT_TIMEZONE AS c_timestamp_without_timezone,"
            + "C_TIMESTAMP_WITH_TIMEZONE AS c_timestamp_with_timezone "
            + "FROM t_source";

    private Connection db2Conn;
    private MongoClient mongoClient;
    private String mongoDatabaseName;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    private static ReplicadbMongodbContainer mongoContainer;

    @BeforeAll
    static void setUp() {
        mongoContainer = ReplicadbMongodbContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        final MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(mongoContainer.getMongoConnectionString())
                .compressorList(List.of())
                .build();
        this.mongoClient = MongoClients.create(clientSettings);
        this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").deleteMany(new Document());
        this.mongoClient.close();
        this.db2Conn.close();
    }

    public int countSinkRows() {
        return (int) this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").countDocuments();
    }

    @Test
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testMongoDbConnection() {
        long count = mongoClient.getDatabase(mongoDatabaseName).getCollection("t_source").countDocuments();
        LOG.info("MongoDB t_source count: {}", count);
        assertTrue(count > 0);
    }

    @Test
    void testDb2Init() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int count = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, count);
    }

    @Test
    void testDb22MongoComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
            "--source-query", SOURCE_QUERY,
                "--sink-connect", mongoContainer.getReplicaSetUrl()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MongoCompleteAtomic() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--source-query", SOURCE_QUERY,
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(1, ReplicaDB.processReplica(options)); // Expect failure - MongoDB doesn't support complete-atomic as sink
    }

    @Test
    void testDb22MongoIncremental() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
            "--source-query", SOURCE_QUERY,
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MongoCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
            "--source-query", SOURCE_QUERY,
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testDb22MongoCompleteAtomicParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--source-query", SOURCE_QUERY,
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(1, ReplicaDB.processReplica(options)); // Expect failure - MongoDB doesn't support complete-atomic as sink
    }

    @Test
    void testDb22MongoIncrementalParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
            "--source-query", SOURCE_QUERY,
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
