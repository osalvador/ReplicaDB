package org.replicadb.oracle;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.replicadb.config.ReplicadbOracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Testcontainers
class Oracle2MongoTest {
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int TOTAL_SINK_ROWS = 4096;
	// Base columns without XML
	private static final String SOURCE_COLUMNS_BASE = "c_integer as \"c_integer\"" + ",c_smallint as \"c_smallint\""
			+ ",c_bigint as \"c_bigint\"" + ",c_numeric as \"c_numeric\"" + ",c_decimal as \"c_decimal\""
			+ ",c_real as \"c_real\"" + ",c_double_precision as \"c_double_precision\"" + ",c_float as \"c_float\""
			+ ",c_binary as \"c_binary\"" + ",c_binary_var as \"c_binary_var\"" + ",c_binary_lob as \"c_binary_lob\""
			+ ",c_boolean as \"c_boolean\"" + ",c_character as \"c_character\""
			+ ",c_character_var as \"c_character_var\"" + ",c_character_lob as \"c_character_lob\""
			+ ",c_national_character as \"c_national_character\""
			+ ",c_national_character_var as \"c_national_character_var\"" + ",c_date as \"c_date\""
			+ ",c_timestamp_without_timezone as \"c_timestamp_without_timezone\""
			+ ",c_timestamp_with_timezone as \"c_timestamp_with_timezone\"" + ",c_interval_day as \"c_interval_day\""
			+ ",c_interval_year as \"c_interval_year\"";
	
	// Alias all columns to lowercase to match MongoDB column names and uniq key index
	// This will be set dynamically based on XML support availability
	private static String sourceColumns;

	private MongoClient mongoClient;
	private String mongoDatabaseName;
	private Connection oracleConn;
	private static ReplicadbMongodbContainer mongoContainer;
	private static ReplicadbOracleContainer oracle;

	@BeforeAll
	static void setUp() {
		// Initialize containers manually for better error handling
		mongoContainer = ReplicadbMongodbContainer.getInstance();
		oracle = ReplicadbOracleContainer.getInstance();
		
		// Set source columns based on XML support availability
		if (oracle.isXmlSupportAvailable()) {
			sourceColumns = SOURCE_COLUMNS_BASE + ",c_xml as \"c_xml\"";
		} else {
			sourceColumns = SOURCE_COLUMNS_BASE;
			System.out.println("Oracle XML support not available - excluding c_xml column from tests");
		}
	}

	@BeforeEach
	void before() throws SQLException {
		// Use compression-disabled connection for ARM64 compatibility
		final MongoClientSettings clientSettings = MongoClientSettings.builder()
				.applyConnectionString(mongoContainer.getMongoConnectionString()).compressorList(List.of()) // Disable
																											// all
																											// compression
																											// to avoid
																											// Snappy
																											// issues on
																											// ARM64
				.build();
		this.mongoClient = MongoClients.create(clientSettings);
		this.mongoDatabaseName = mongoContainer.getMongoConnectionString().getDatabase();
		this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").deleteMany(new Document());
		this.mongoClient.close();
		this.oracleConn.close();
	}

	public int countSinkRows() {
		// Count rows in sink table
		return (int) this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").countDocuments();
	}

	@Test
	void testOracle2MongodbComplete() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns", sourceColumns};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2MongodbCompleteAtomic() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns", sourceColumns, "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(1, ReplicaDB.processReplica(options));
	}

	@Test
	void testOracle2MongodbIncremental() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns", sourceColumns, "--mode",
				ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2MongodbCompleteParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns", sourceColumns, "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2MongodbIncrementalParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns", sourceColumns, "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}
}
