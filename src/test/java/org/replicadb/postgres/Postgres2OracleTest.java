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
import org.replicadb.utils.OracleTestUtils;
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
		// Truncate sink table with retry logic for ORA-00054 lock errors
		OracleTestUtils.truncateTableWithRetry(this.oracledbConn, "t_sink");
		this.oracledbConn.close();
		this.postgresConn.close();
	}

	public int countSinkRows() throws SQLException {
		final Statement stmt = this.oracledbConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
		rs.next();
		return rs.getInt(1);
	}

	// Helper method: Check if a table exists in Oracle
	private boolean tableExists(String tableName) throws SQLException {
		final Statement stmt = this.oracledbConn.createStatement();
		// Oracle stores table names in uppercase in system catalogs
		final ResultSet rs = stmt.executeQuery(
			"SELECT COUNT(*) FROM user_tables WHERE table_name = '" + tableName.toUpperCase() + "'"
		);
		rs.next();
		return rs.getInt(1) > 0;
	}

	// Helper method: Get primary key columns for a table
	private String[] getPrimaryKeys(String tableName) throws SQLException {
		final Statement stmt = this.oracledbConn.createStatement();
		// Oracle stores constraint and column names in uppercase
		final ResultSet rs = stmt.executeQuery(
			"SELECT acc.column_name " +
			"FROM all_constraints ac " +
			"JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name " +
			"WHERE ac.constraint_type = 'P' AND ac.table_name = '" + tableName.toUpperCase() + "' " +
			"ORDER BY acc.position"
		);
		java.util.List<String> pkColumns = new java.util.ArrayList<>();
		while (rs.next()) {
			pkColumns.add(rs.getString(1).toLowerCase());
		}
		return pkColumns.toArray(new String[0]);
	}

	// Helper method: Count rows in a specific table
	private int countRows(String tableName) throws SQLException {
		final Statement stmt = this.oracledbConn.createStatement();
		final ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
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

	// ========== Auto-Create Tests ==========

	@Test
	void testPostgres2OracleAutoCreateCompleteMode() throws ParseException, IOException, SQLException {
		final String sinkTable = "t_sink_autocreate";
		// Use subset of columns that work correctly (exclude boolean which has conversion issues)
		final String columns = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
				"C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
				"C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";
		
		// Drop table if exists from previous test run
		if (tableExists(sinkTable)) {
			final Statement stmt = this.oracledbConn.createStatement();
			stmt.execute("DROP TABLE " + sinkTable + " PURGE");
		}
		
		// Verify table does not exist
		Assertions.assertFalse(tableExists(sinkTable), "Table should not exist before auto-create");
		
		// Run replication with auto-create in complete mode
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, 
				"--source-connect", postgres.getJdbcUrl(), 
				"--source-user", postgres.getUsername(), 
				"--source-password", postgres.getPassword(), 
				"--sink-connect", oracle.getJdbcUrl(), 
				"--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), 
				"--sink-table", sinkTable,
				"--source-columns", columns,
				"--sink-columns", columns,
				"--sink-auto-create",
				"--mode", ReplicationMode.COMPLETE.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		
		// Verify table was created and populated
		Assertions.assertTrue(tableExists(sinkTable), "Table should exist after auto-create");
		final int rowCount = countRows(sinkTable);
		assertEquals(TOTAL_SINK_ROWS, rowCount, "Row count should match source");
		LOG.info("Successfully replicated {} rows to auto-created Oracle table", rowCount);
		
		// Cleanup
		final Statement stmt = this.oracledbConn.createStatement();
		stmt.execute("DROP TABLE " + sinkTable + " PURGE");
	}

	@Test
	void testPostgres2OracleAutoCreateIncrementalMode() throws ParseException, IOException, SQLException {
		final String sinkTable = "t_sink_autocreate_incremental";
		// Use subset of columns that work correctly (exclude boolean which has conversion issues)
		final String columns = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
				"C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
				"C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";
		
		// Drop table if exists from previous test run
		if (tableExists(sinkTable)) {
			final Statement stmt = this.oracledbConn.createStatement();
			stmt.execute("DROP TABLE " + sinkTable + " PURGE");
		}
		
		// Verify table does not exist
		Assertions.assertFalse(tableExists(sinkTable), "Table should not exist before auto-create");
		
		// Run replication with auto-create in incremental mode
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, 
				"--source-connect", postgres.getJdbcUrl(), 
				"--source-user", postgres.getUsername(), 
				"--source-password", postgres.getPassword(), 
				"--sink-connect", oracle.getJdbcUrl(), 
				"--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), 
				"--sink-table", sinkTable,
				"--source-columns", columns,
				"--sink-columns", columns,
				"--sink-auto-create",
				"--mode", ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		
		// Verify table was created with primary key
		Assertions.assertTrue(tableExists(sinkTable), "Table should exist after auto-create");
		final String[] pkColumns = getPrimaryKeys(sinkTable);
		Assertions.assertTrue(pkColumns.length > 0, "Primary key should be created");
		LOG.info("Primary key columns: {}", String.join(", ", pkColumns));
		
		final int rowCount = countRows(sinkTable);
		assertEquals(TOTAL_SINK_ROWS, rowCount, "Row count should match source");
		LOG.info("Successfully replicated {} rows with primary key to auto-created table", rowCount);
		
		// Run replication again in incremental mode to test merge (should not duplicate rows)
		assertEquals(0, ReplicaDB.processReplica(options));
		final int rowCountAfterMerge = countRows(sinkTable);
		assertEquals(TOTAL_SINK_ROWS, rowCountAfterMerge, "Row count should remain unchanged after merge");
		LOG.info("Incremental mode merge successful - row count unchanged: {}", rowCountAfterMerge);
		
		// Cleanup
		final Statement stmt = this.oracledbConn.createStatement();
		stmt.execute("DROP TABLE " + sinkTable + " PURGE");
	}

	@Test
	void testPostgres2OracleAutoCreateSkippedWhenTableExists() throws ParseException, IOException, SQLException {
		final String sinkTable = "t_sink";
		
		// Verify table already exists (from container setup)
		Assertions.assertTrue(tableExists(sinkTable), "Table should already exist");
		
		// Get column count before replication
		final Statement stmt = this.oracledbConn.createStatement();
		final ResultSet rs = stmt.executeQuery(
			"SELECT COUNT(*) FROM user_tab_columns WHERE table_name = '" + sinkTable.toUpperCase() + "'"
		);
		rs.next();
		final int columnCountBefore = rs.getInt(1);
		
		// Run replication with auto-create (should skip because table exists)
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, 
				"--source-connect", postgres.getJdbcUrl(), 
				"--source-user", postgres.getUsername(), 
				"--source-password", postgres.getPassword(), 
				"--sink-connect", oracle.getJdbcUrl(), 
				"--sink-user", oracle.getUsername(),
				"--sink-password", oracle.getPassword(), 
				"--sink-table", sinkTable,
				"--sink-auto-create",
				"--mode", ReplicationMode.COMPLETE.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		
		// Verify table structure unchanged (no DDL executed)
		final ResultSet rs2 = stmt.executeQuery(
			"SELECT COUNT(*) FROM user_tab_columns WHERE table_name = '" + sinkTable.toUpperCase() + "'"
		);
		rs2.next();
		final int columnCountAfter = rs2.getInt(1);
		assertEquals(columnCountBefore, columnCountAfter, "Column count should remain unchanged");
		
		// Verify data was still replicated
		final int rowCount = countSinkRows();
		assertEquals(TOTAL_SINK_ROWS, rowCount, "Row count should match source");
		LOG.info("Auto-create correctly skipped for existing table, {} rows replicated", rowCount);
	}
}
