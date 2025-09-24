package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test class for Oracle SDO_GEOMETRY spatial data type replication.
 * 
 * Note: These tests will be automatically skipped when using Oracle Free edition,
 * as it doesn't support spatial data types (SDO_GEOMETRY).
 * 
 * To run these tests, use Oracle Enterprise or Standard edition container.
 */
@Testcontainers
class Oracle2OracleSdoGeomTest {
	private static final Logger LOG = LogManager.getLogger(Oracle2OracleSdoGeomTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";

	private static final int EXPECTED_ROWS = 4;
	private static final String SOURCE_TABLE = "t_source_geo";
	private static final String SINK_TABLE = "t_sink_geo";

	private Connection oracleConn;
	private static ReplicadbOracleContainer oracle;

	/**
	 * Static method to determine if Oracle geometry tests should be disabled.
	 * This is used by @DisabledIf annotation.
	 */
	static boolean isOracleGeometryDisabled() {
		// Check system property to force disable geometry tests
		if ("true".equals(System.getProperty("replicadb.test.oracle.geometry.disable"))) {
			return true;
		}
		
		try {
			// Try to get Oracle container instance
			ReplicadbOracleContainer testOracle = ReplicadbOracleContainer.getInstance();
			
			// If Oracle Free edition or geometry not supported, disable tests
			if (testOracle.isOracleFreeEdition() || !testOracle.isGeometrySupportAvailable()) {
				return true;
			}
			
			return false;
		} catch (Exception e) {
			// If container can't be started, disable tests
			return true;
		}
	}

	/**
	 * Static method to determine if Oracle geometry tests should be enabled.
	 * This is used by @EnabledIf annotation.
	 */
	static boolean isOracleGeometryEnabled() {
		return !isOracleGeometryDisabled();
	}

	@BeforeAll
	static void setUp() {
		try {
			// Initialize Oracle container manually for better error handling
			oracle = ReplicadbOracleContainer.getInstance();
			
			// Skip all tests if Oracle Free edition is used or geometry support is not available
			assumeTrue(isOracleGeometryEnabled(), 
				"Skipping Oracle SDO_GEOMETRY tests - Oracle Free edition doesn't support spatial data types. "
				+ "To run these tests, use Oracle Enterprise/Standard edition container or set system property "
				+ "replicadb.test.oracle.geometry.disable=false");
		} catch (Exception e) {
			// If Oracle container fails to start (Docker issues, missing image, etc.)
			// skip all tests with a clear message
			assumeTrue(false, 
				"Skipping Oracle SDO_GEOMETRY tests - Oracle container could not be started: " + e.getMessage());
		}
	}

	@BeforeEach
	void before() throws SQLException {
		this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.oracleConn.createStatement().execute("TRUNCATE TABLE T_SINK_GEO");
		this.oracleConn.close();
	}

	public int countSinkRows() throws SQLException {
		final Statement stmt = this.oracleConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_sink_geo");
		rs.next();
		return rs.getInt(1);
	}

	@Test
	void testOracleConnection() throws SQLException {
		final Connection conn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(),
				oracle.getPassword());
		final Statement stmt = conn.createStatement();
		final ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
		rs.next();
		final String version = rs.getString(1);
		LOG.info(version);
		assertTrue(version.contains("1"));
		conn.close();
	}

	@Test
	void testOracleInit() throws SQLException {
		try {
			final Statement stmt = this.oracleConn.createStatement();
			final ResultSet rs = stmt.executeQuery("select count(*) from t_source_geo");
			rs.next();
			final int rows = rs.getInt(1);
			assertEquals(EXPECTED_ROWS, rows);
		} catch (SQLException e) {
			if (e.getMessage().contains("ORA-00942") || e.getMessage().contains("table or view does not exist")) {
				assumeTrue(false, "Geometry table t_source_geo doesn't exist - SDO_GEOMETRY not supported in Oracle Free edition");
			}
			throw e;
		}
	}

	@Test
	void testOracle2OracleComplete() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--source-table", SOURCE_TABLE, "--sink-table", SINK_TABLE};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2OracleCompleteAtomic() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--source-table", SOURCE_TABLE, "--sink-table",
				SINK_TABLE};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2OracleIncremental() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--source-table", SOURCE_TABLE, "--sink-table", SINK_TABLE};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2OracleCompleteParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--jobs", "4", "--source-table", SOURCE_TABLE, "--sink-table", SINK_TABLE};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2OracleCompleteAtomicParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--jobs", "4", "--source-table", SOURCE_TABLE,
				"--sink-table", SINK_TABLE};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2OracleIncrementalParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4", "--source-table", SOURCE_TABLE,
				"--sink-table", SINK_TABLE};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}
}
