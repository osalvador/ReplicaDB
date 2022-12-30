package org.replicadb.oracle;

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
import org.replicadb.config.ReplicadbOracleContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Oracle2MongoTest {
   private static final Logger LOG = LogManager.getLogger(Oracle2MongoTest.class);
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
   private static final int TOTAL_SINK_ROWS = 4096;
   // Alias all columns to lowercase to match MongoDB column names and uniq key index
   private static final String SOURCE_COLUMNS =
       "c_integer as \"c_integer\"" +
       ",c_smallint as \"c_smallint\"" +
       ",c_bigint as \"c_bigint\"" +
       ",c_numeric as \"c_numeric\"" +
       ",c_decimal as \"c_decimal\"" +
       ",c_real as \"c_real\"" +
       ",c_double_precision as \"c_double_precision\"" +
       ",c_float as \"c_float\"" +
       ",c_binary as \"c_binary\"" +
       ",c_binary_var as \"c_binary_var\"" +
       ",c_binary_lob as \"c_binary_lob\"" +
       ",c_boolean as \"c_boolean\"" +
       ",c_character as \"c_character\"" +
       ",c_character_var as \"c_character_var\"" +
       ",c_character_lob as \"c_character_lob\"" +
       ",c_national_character as \"c_national_character\"" +
       ",c_national_character_var as \"c_national_character_var\"" +
       ",c_date as \"c_date\"" +
       ",c_timestamp_without_timezone as \"c_timestamp_without_timezone\"" +
       ",c_timestamp_with_timezone as \"c_timestamp_with_timezone\"" +
       ",c_interval_day as \"c_interval_day\"" +
       ",c_interval_year as \"c_interval_year\"" +
       ",c_xml as \"c_xml\"";

   private MongoClient mongoClient;
   private String mongoDatabaseName;
   private Connection oracleConn;

   @Rule
   public static ReplicadbMongodbContainer mongoContainer = ReplicadbMongodbContainer.getInstance();

   @Rule
   public static OracleContainer oracle = ReplicadbOracleContainer.getInstance();

   @BeforeAll
   static void setUp () {
   }

   @BeforeEach
   void before () throws SQLException {
      this.mongoClient = MongoClients.create(mongoContainer.getMongoConnectionString());
      this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
      this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
   }

   @AfterEach
   void tearDown () throws SQLException {
      // Truncate sink table and close connections
      this.mongoClient.getDatabase(mongoDatabaseName).getCollection("t_sink").deleteMany(new Document());
      this.mongoClient.close();
      this.oracleConn.close();
   }


   public int countSinkRows () throws SQLException {
      // Count rows in sink table
      return (int) this.mongoClient.getDatabase(mongoDatabaseName).getCollection("t_sink").countDocuments();
   }

   @Test
   void testOracle2MongodbComplete () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", oracle.getJdbcUrl(),
          "--source-user", oracle.getUsername(),
          "--source-password", oracle.getPassword(),
          "--sink-connect", mongoContainer.getReplicaSetUrl(),
          "--source-columns", SOURCE_COLUMNS
      };
      ToolOptions options = new ToolOptions(args);
      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

    @Test
    void testOracle2MongodbCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--source-columns", SOURCE_COLUMNS,
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(1, ReplicaDB.processReplica(options));
    }

    @Test
    void testOracle2MongodbIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--source-columns", SOURCE_COLUMNS,
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

    }

    @Test
    void testOracle2MongodbCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--source-columns", SOURCE_COLUMNS,
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }


    @Test
    void testOracle2MongodbIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", oracle.getJdbcUrl(),
                "--source-user", oracle.getUsername(),
                "--source-password", oracle.getPassword(),
                "--sink-connect", mongoContainer.getReplicaSetUrl(),
                "--source-columns", SOURCE_COLUMNS,
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }
}