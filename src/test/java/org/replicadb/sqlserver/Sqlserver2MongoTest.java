package org.replicadb.sqlserver;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Sqlserver2MongoTest {
    private static final Logger LOG = LogManager.getLogger(Sqlserver2MongoTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection sqlserverConn;
    private MongoClient mongoClient;
    private String mongoDatabaseName;

    @Rule
    public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();
    private static ReplicadbMongodbContainer mongoContainer;

    @BeforeAll
    static void setUp() {
        mongoContainer = ReplicadbMongodbContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
        // Use compression-disabled connection for ARM64 compatibility
        final MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(mongoContainer.getMongoConnectionString())
                .compressorList(List.of())
                .build();
        this.mongoClient = MongoClients.create(clientSettings);
        this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clear MongoDB sink collection
        this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").deleteMany(new Document());
        this.mongoClient.close();
        this.sqlserverConn.close();
    }

    public int countSinkRows() {
        return (int) this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").countDocuments();
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
    void testMongoDBConnection() {
        long count = mongoClient.getDatabase(mongoDatabaseName).getCollection("t_source").countDocuments();
        LOG.info("MongoDB t_source count: {}", count);
        assertTrue(count > 0);
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
    void testSqlserver2MongoComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testSqlserver2MongoCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", sqlserver.getJdbcUrl(),
                "--source-user", sqlserver.getUsername(),
                "--source-password", sqlserver.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
