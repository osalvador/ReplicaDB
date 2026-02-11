package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class Oracle2PostgresTest {
	private static final Logger LOG = LogManager.getLogger(Oracle2PostgresTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int EXPECTED_ROWS = 4096;
	// Skip BINARY and REAL/FLOAT columns due to binary COPY format incompatibilities
	// Include Oracle-specific columns: INTERVAL_DAY, INTERVAL_YEAR
	private static final String SINK_COLUMNS = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL," +
			"C_BOOLEAN," +
			"C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
			"C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
			"C_DATE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE," +
			"C_INTERVAL_DAY,C_INTERVAL_YEAR";
	private static final String SOURCE_COLUMNS = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL," +
			"C_BOOLEAN," +
			"C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
			"C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
			"C_DATE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE," +
			"C_INTERVAL_DAY,C_INTERVAL_YEAR";

	private Connection oracleConn;
	private Connection postgresConn;
	private static ReplicadbOracleContainer oracle;
	private static ReplicadbPostgresqlContainer postgres;

	@BeforeAll
	static void setUp() {
		// Initialize containers manually for better error handling
		oracle = ReplicadbOracleContainer.getInstance();
		postgres = ReplicadbPostgresqlContainer.getInstance();
	}

	@BeforeEach
	void before() throws SQLException {
		this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
		this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
				postgres.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
		this.oracleConn.close();
		this.postgresConn.close();
	}

	public int countSinkRows() throws SQLException {
		final Statement stmt = this.postgresConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
		rs.next();
		return rs.getInt(1);
	}

	@Test
	void testOracleConnection() throws SQLException {
		final Statement stmt = this.oracleConn.createStatement();
		final ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
		rs.next();
		final String version = rs.getString(1);
		LOG.info(version);
		assertTrue(version.contains("1"));
	}

	@Test
	void testPostgresConnection() throws SQLException {
		final Statement stmt = this.postgresConn.createStatement();
		final ResultSet rs = stmt.executeQuery("SELECT 1");
		rs.next();
		final String version = rs.getString(1);
		LOG.info(version);
		assertTrue(version.contains("1"));
	}

	@Test
	void testOracleInit() throws SQLException {
		final Statement stmt = this.oracleConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_source");
		rs.next();
		final int count = rs.getInt(1);
		assertEquals(EXPECTED_ROWS, count);
	}

	@Test
	void testOracle2PostgresComplete() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
				postgres.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
				postgres.getPassword(), "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2PostgresIncremental() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
				postgres.getPassword(), "--sink-staging-schema", "PUBLIC", "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
				postgres.getPassword(), "--jobs", "4", "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
				postgres.getPassword(), "--sink-staging-schema", "PUBLIC", "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--jobs", "4", "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
				postgres.getPassword(), "--sink-staging-schema", "PUBLIC", "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4", "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testFlashbackQuerySCNCapture() throws ParseException, IOException, SQLException {
		// Setup log capture
		final StringWriter logOutput = new StringWriter();
		final LoggerContext context = (LoggerContext) LogManager.getContext(false);
		final PatternLayout layout = PatternLayout.newBuilder()
				.withPattern("%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n")
				.build();
		final WriterAppender appender = WriterAppender.newBuilder()
				.setName("TestAppender")
				.setTarget(logOutput)
				.setLayout(layout)
				.build();
		appender.start();
		context.getConfiguration().addAppender(appender);
		context.getConfiguration().getRootLogger().addAppender(appender, null, null);
		context.updateLoggers();

		try {
			// Verify V$DATABASE is accessible and SCN can be captured
			final Statement stmt = this.oracleConn.createStatement();
			final ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE");
			assertTrue(rs.next(), "Should be able to query V$DATABASE for SCN");
			final long scn = rs.getLong(1);
			assertTrue(scn > 0, "SCN should be positive");
			LOG.info("Current Oracle SCN before replication: {}", scn);

			// Execute replication with parallel jobs (should use flashback query)
			final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
					oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
					"--sink-connect", postgres.getJdbcUrl(), "--sink-user", postgres.getUsername(), "--sink-password",
					postgres.getPassword(), "--jobs", "4", "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS};
			final ToolOptions options = new ToolOptions(args);
			assertEquals(0, ReplicaDB.processReplica(options));
			assertEquals(EXPECTED_ROWS, this.countSinkRows());

			// Verify log contains SCN capture message
			final String logs = logOutput.toString();
			assertTrue(logs.contains("Captured Oracle SCN for consistent read:"),
					"Log should contain message about capturing SCN for flashback query. Logs:\n" + logs);
			
			LOG.info("✓ Flashback query test passed - SCN was captured and used for consistent read");

		} finally {
			// Cleanup log appender
			context.getConfiguration().getRootLogger().removeAppender("TestAppender");
			appender.stop();
			context.updateLoggers();
		}
	}
}
