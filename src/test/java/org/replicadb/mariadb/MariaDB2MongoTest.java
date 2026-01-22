package org.replicadb.mariadb;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MariaDB2MongoTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private Connection mariadbConn;
    private MongoClient mongoClient;
    private String mongoDatabaseName;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    private static ReplicadbMongodbContainer mongoContainer;

    @BeforeAll
    static void setUp() {
        mongoContainer = ReplicadbMongodbContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
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
        this.mariadbConn.close();
    }

    public int countSinkRows() {
        return (int) this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").countDocuments();
    }

    @Test
    void testMariadbVersion102() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("10.2"));
    }

    @Test
    void testMongoDBConnection() {
        long count = mongoClient.getDatabase(mongoDatabaseName).getCollection("t_source").countDocuments();
        LOG.info("MongoDB t_source count: {}", count);
        assertTrue(count > 0);
    }

    @Test
    void testMariaDB2MongoComplete() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2MongoCompleteParallel() throws ParseException, IOException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
