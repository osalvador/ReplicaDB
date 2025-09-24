package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.config.ReplicadbSqliteFakeContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class OracleSqliteTest {
	private static final Logger LOG = LogManager.getLogger(OracleSqliteTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int TOTAL_SINK_ROWS = 4096;

	private Connection sqliteConn;
	private Connection oracleConn;
	private static ReplicadbSqliteFakeContainer sqlite;
	private static ReplicadbOracleContainer oracle;

	@BeforeAll
	static void setUp() {
		// Initialize SQLite fake container manually
		sqlite = ReplicadbSqliteFakeContainer.getInstance();

		// Initialize Oracle container manually to handle potential startup issues
		try {
			oracle = ReplicadbOracleContainer.getInstance();
		} catch (final Exception e) {
			LOG.error("Failed to initialize Oracle container: {}", e.getMessage(), e);
			throw new RuntimeException("Oracle container initialization failed", e);
		}
	}

	@BeforeEach
	void before() throws SQLException {
		this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
		this.sqliteConn = DriverManager.getConnection(sqlite.getJdbcUrl());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		try (final Statement stmt = this.sqliteConn.createStatement()) {
			stmt.execute("DELETE FROM t_sink");
		}
		this.sqliteConn.close();
		this.oracleConn.close();
	}

	public int countSinkRows() throws SQLException {
		try (final Statement stmt = this.sqliteConn.createStatement();
				final ResultSet rs = stmt.executeQuery("select count(*) from t_sink")) {
			rs.next();
			final int count = rs.getInt(1);
			LOG.info("SQLite sink total rows: {}", count);
			return count;
		}
	}

	@Test
	void testOracle2SqliteComplete() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlite.getJdbcUrl()};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqliteCompleteAtomic() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlite.getJdbcUrl(), "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		// SQLite doesn't support complete-atomic mode, should return error code 1
		assertEquals(1, ReplicaDB.processReplica(options));
	}

	@Test
	void testOracle2SqliteIncremental() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlite.getJdbcUrl(), "--sink-staging-schema", sqlite.getDatabaseName(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqliteCompleteParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlite.getJdbcUrl(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqliteCompleteAtomicParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlite.getJdbcUrl(), "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
				"--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		// SQLite doesn't support complete-atomic mode, should return error code 1
		assertEquals(1, ReplicaDB.processReplica(options));
	}

	@Test
	void testOracle2SqliteIncrementalParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlite.getJdbcUrl(), "--sink-staging-schema", sqlite.getDatabaseName(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}
}
