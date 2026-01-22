package org.replicadb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.replicadb.config.ReplicadbOracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Mongo2OracleTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;
    private static final String SINK_COLLECTION = "t_sink";
    private static final String SOURCE_COLUMNS = "{_id:0,c_integer:1,c_smallint:1,c_bigint:1,c_numeric:1,c_decimal:1,c_real:1,c_double_precision:1,c_float:1,c_binary:1,c_binary_var:1,c_binary_lob:1,c_boolean:1,c_character:1,c_character_var:1,c_character_lob:1,c_national_character:1,c_national_character_var:1,c_date:1,c_timestamp_with_timezone:1}";
    private static final String SINK_COLUMNS = "C_INTEGER, C_SMALLINT, C_BIGINT, C_NUMERIC, C_DECIMAL, C_REAL, C_DOUBLE_PRECISION, C_FLOAT, C_BINARY, C_BINARY_VAR, C_BINARY_LOB, C_BOOLEAN, C_CHARACTER, C_CHARACTER_VAR, C_CHARACTER_LOB, C_NATIONAL_CHARACTER, C_NATIONAL_CHARACTER_VAR, C_DATE, C_TIMESTAMP_WITH_TIMEZONE";

    private MongoClient mongoClient;
    private String mongoDatabaseName;
    private Connection oracleConn;
    private static ReplicadbOracleContainer oracle;

    @Rule
    public static ReplicadbMongodbContainer mongoContainer = ReplicadbMongodbContainer.getInstance();

    @BeforeAll
    static void setUp() {
        oracle = ReplicadbOracleContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.mongoClient = MongoClients.create(mongoContainer.getMongoConnectionString());
        this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
        this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        mongoClient.getDatabase(mongoDatabaseName).getCollection(SINK_COLLECTION).deleteMany(new Document());
        oracleConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.mongoClient.close();
        this.oracleConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        return rs.getInt(1);
    }

    @Test
    void testMongodb2OracleComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMongodb2OracleCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS,
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMongodb2OracleCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS,
                "--sink-staging-schema", oracle.getUsername(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMongodb2OracleIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mongoContainer.getReplicaSetUrl(),
                "--sink-connect", oracle.getJdbcUrl(),
                "--sink-user", oracle.getUsername(),
                "--sink-password", oracle.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS,
                "--sink-staging-schema", oracle.getUsername(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}
