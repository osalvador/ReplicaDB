package org.replicadb.postgres;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMongodbContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Postgres2MongoTest {
	private static final Logger LOG = LogManager.getLogger(Postgres2MongoTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int TOTAL_SINK_ROWS = 4097;
	private static final String SOURCE_COLUMNS = "C_XML,C_JSON, C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE,C_INTERVAL_DAY,C_INTERVAL_YEAR,"
			+ "C_MULTIDIMENSIONAL_ARRAY::text as C_MULTIDIMENSIONAL_ARRAY," + "C_ARRAY::text as C_ARRAY";

	private MongoClient mongoClient;
	private String mongoDatabaseName;
	private Connection postgresConn;
	private static ReplicadbMongodbContainer mongoContainer;
	private static ReplicadbPostgresqlContainer postgres;

	@BeforeAll
	static void setUp() {
		// Initialize containers manually for better error handling
		mongoContainer = ReplicadbMongodbContainer.getInstance();
		postgres = ReplicadbPostgresqlContainer.getInstance();
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
		this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
				postgres.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").deleteMany(new Document());
		this.mongoClient.close();
		this.postgresConn.close();
	}

	public int countSinkRows() {
		// Count rows in sink table - removed unnecessary SQLException
		return (int) this.mongoClient.getDatabase(this.mongoDatabaseName).getCollection("t_sink").countDocuments();
	}

	@Test
	void testPostgres2MongodbComplete() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns",
				SOURCE_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2MongodbCompleteAtomic() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns",
				SOURCE_COLUMNS, "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(1, ReplicaDB.processReplica(options));
	}

	@Test
	void testPostgres2MongodbIncremental() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns",
				SOURCE_COLUMNS, "--mode", ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2MongodbCompleteParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns",
				SOURCE_COLUMNS, "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testMongodb2PostgresIncrementalParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", mongoContainer.getReplicaSetUrl(), "--source-columns",
				SOURCE_COLUMNS, "--mode", ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}
}