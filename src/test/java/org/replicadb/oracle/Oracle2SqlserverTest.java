package org.replicadb.oracle;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbOracleContainer;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Oracle2SqlserverTest {
	private static final Logger LOG = LogManager.getLogger(Oracle2SqlserverTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int EXPECTED_ROWS = 4097;
	// Oracle source doesn't have c_time_without_timezone, so we exclude it from mapping
	private static final String SOURCE_COLUMNS = "c_integer,c_smallint,c_bigint,c_numeric,c_decimal,"
			+ "c_real,c_double_precision,c_float,c_binary,c_binary_var,c_binary_lob,"
			+ "c_boolean,c_character,c_character_var,c_character_lob,c_national_character,"
			+ "c_national_character_var,c_date,c_timestamp_without_timezone,"
			+ "c_xml";
	// Match source columns - skip c_time_without_timezone which Oracle doesn't have
	private static final String SINK_COLUMNS = "c_integer,c_smallint,c_bigint,c_numeric,c_decimal,"
			+ "c_real,c_double_precision,c_float,c_binary,c_binary_var,c_binary_lob,"
			+ "c_boolean,c_character,c_character_var,c_character_lob,c_national_character,"
			+ "c_national_character_var,c_date,c_timestamp_without_timezone,"
			+ "c_xml";

	private Connection oracleConn;
	private Connection sqlserverConn;
	private static ReplicadbOracleContainer oracle;
	private static ReplicadbSqlserverContainer sqlserver;

	@BeforeAll
	static void setUp() {
		// Initialize containers manually for better error handling
		oracle = ReplicadbOracleContainer.getInstance();
		sqlserver = ReplicadbSqlserverContainer.getInstance();
	}

	@BeforeEach
	void before() throws SQLException {
		this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
		this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(),
				sqlserver.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
		this.oracleConn.close();
		this.sqlserverConn.close();
	}

	public int countSinkRows() throws SQLException {
		final Statement stmt = this.sqlserverConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
		rs.next();
		return rs.getInt(1);
	}

	private boolean tableExists(Connection conn, String tableName) throws SQLException {
		DatabaseMetaData meta = conn.getMetaData();
		try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
			return rs.next();
		}
	}

	private int countRows(Connection conn, String tableName) throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
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
	void testSqlserverConnection() throws SQLException {
		final Statement stmt = this.sqlserverConn.createStatement();
		final ResultSet rs = stmt.executeQuery("SELECT @@VERSION");
		rs.next();
		final String version = rs.getString(1);
		LOG.info(version);
		assertTrue(version.contains("Microsoft"));
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
	void testOracle2SqlserverComplete() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqlserverCompleteAtomic() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--sink-staging-schema", "dbo", "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqlserverIncremental() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--sink-staging-schema", "dbo", "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqlserverCompleteParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--jobs", "4", "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqlserverCompleteAtomicParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--sink-staging-schema", "dbo", "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--jobs", "4", "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqlserverIncrementalParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--sink-staging-schema", "dbo", "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4", "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testOracle2SqlserverAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
		String sinkTable = "t_sink_autocreate_oracle2sqlserver";
		Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--sink-table", sinkTable, "--sink-auto-create", "true",
				"--mode", ReplicationMode.COMPLETE.getModeText(), "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertTrue(tableExists(sqlserverConn, sinkTable), "Sink table should exist after auto-create");
		assertEquals(EXPECTED_ROWS, countRows(sqlserverConn, sinkTable));

		// Cleanup
		sqlserverConn.createStatement().execute("DROP TABLE " + sinkTable);
	}

	@Test
	void testOracle2SqlserverAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
		String sinkTable = "t_sink_autocreate_oracle2sqlserver_incr";
		Assertions.assertFalse(tableExists(sqlserverConn, sinkTable), "Sink table should not exist before test");

		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--sink-table", sinkTable, "--sink-staging-schema", "dbo",
				"--sink-auto-create", "true", "--mode", ReplicationMode.INCREMENTAL.getModeText(), "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertTrue(tableExists(sqlserverConn, sinkTable), "Sink table should exist after auto-create");
		assertEquals(EXPECTED_ROWS, countRows(sqlserverConn, sinkTable));

		// Test merge by running again
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, countRows(sqlserverConn, sinkTable), "Row count should remain same after merge");

		// Cleanup
		sqlserverConn.createStatement().execute("DROP TABLE " + sinkTable);
	}

	@Test
	void testOracle2SqlserverAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
		String sinkTable = "t_sink"; // Use existing table

		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				oracle.getJdbcUrl(), "--source-user", oracle.getUsername(), "--source-password", oracle.getPassword(),
				"--sink-connect", sqlserver.getJdbcUrl(), "--sink-user", sqlserver.getUsername(), "--sink-password",
				sqlserver.getPassword(), "--sink-table", sinkTable, "--sink-auto-create", "true",
				"--source-columns", SOURCE_COLUMNS, "--sink-columns", SINK_COLUMNS,
				"--mode", ReplicationMode.COMPLETE.getModeText(), "--fetch-size", "2"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}
}
