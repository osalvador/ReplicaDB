package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Postgres2OracleTest {
	private static final Logger LOG = LogManager.getLogger(Postgres2OracleTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int TOTAL_SINK_ROWS = 4097;

	private Connection oracledbConn;
	private Connection postgresConn;
	private static ReplicadbPostgresqlContainer postgres;
	private static ReplicadbOracleContainer oracle;

	@BeforeAll
	static void setUp() {
		// Initialize containers manually for better error handling
		postgres = ReplicadbPostgresqlContainer.getInstance();
		oracle = ReplicadbOracleContainer.getInstance();
	}

	@BeforeEach
	void before() throws SQLException {
		this.oracledbConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(),
				oracle.getPassword());
		this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
				postgres.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.oracledbConn.createStatement().execute("TRUNCATE TABLE t_sink");
		this.oracledbConn.close();
		this.postgresConn.close();
	}

	public int countSinkRows() throws SQLException {
		final Statement stmt = this.oracledbConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
		rs.next();
		return rs.getInt(1);
	}

	@Test
	void testOracleConnection() throws SQLException {
		final Statement stmt = this.oracledbConn.createStatement();
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
	void testPostgresInit() throws SQLException {
		final Statement stmt = this.postgresConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_source");
		rs.next();
		final int count = rs.getInt(1);
		assertEquals(TOTAL_SINK_ROWS, count);
	}

	@Test
	void testPostgres2OracleComplete() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword()};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2OracleCompleteAtomic() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2OracleIncremental() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2OracleCompleteParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2OracleCompleteAtomicParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testPostgres2OracleIncrementalParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}
}