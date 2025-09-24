package org.replicadb.mysql;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.replicadb.config.ReplicadbOracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2OracleTest {
	private static final Logger LOG = LogManager.getLogger(MySQL2OracleTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int EXPECTED_ROWS = 4097;

	private Connection mysqlConn;
	private Connection oracleConn;
	private static ReplicadbMysqlContainer mysql;
	private static ReplicadbOracleContainer oracle;

	@BeforeAll
	static void setUp() {
		// Initialize containers manually for better error handling
		mysql = ReplicadbMysqlContainer.getInstance();
		oracle = ReplicadbOracleContainer.getInstance();
	}

	@BeforeEach
	void before() throws SQLException {
		this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
		this.oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		// Truncate sink table and close connections
		this.oracleConn.createStatement().execute("TRUNCATE TABLE t_sink");
		this.mysqlConn.close();
		this.oracleConn.close();
	}

	public int countSinkRows() throws SQLException {
		final Statement stmt = this.oracleConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
		rs.next();
		return rs.getInt(1);
	}

	@Test
	void testMysqlConnection() throws SQLException {
		final Statement stmt = this.mysqlConn.createStatement();
		final ResultSet rs = stmt.executeQuery("SELECT 1");
		rs.next();
		final String version = rs.getString(1);
		LOG.info(version);
		assertTrue(version.contains("1"));
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
	void testMysqlInit() throws SQLException {
		final Statement stmt = this.mysqlConn.createStatement();
		final ResultSet rs = stmt.executeQuery("select count(*) from t_source");
		rs.next();
		final int count = rs.getInt(1);
		assertEquals(EXPECTED_ROWS, count);
	}

	@Test
	void testMySQL2OracleComplete() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				mysql.getJdbcUrl(), "--source-user", mysql.getUsername(), "--source-password", mysql.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword()};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testMySQL2OracleCompleteAtomic() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				mysql.getJdbcUrl(), "--source-user", mysql.getUsername(), "--source-password", mysql.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testMySQL2OracleIncremental() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				mysql.getJdbcUrl(), "--source-user", mysql.getUsername(), "--source-password", mysql.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testMySQL2OracleCompleteParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				mysql.getJdbcUrl(), "--source-user", mysql.getUsername(), "--source-password", mysql.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testMySQL2OracleCompleteAtomicParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				mysql.getJdbcUrl(), "--source-user", mysql.getUsername(), "--source-password", mysql.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.COMPLETE_ATOMIC.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}

	@Test
	void testMySQL2OracleIncrementalParallel() throws ParseException, IOException, SQLException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				mysql.getJdbcUrl(), "--source-user", mysql.getUsername(), "--source-password", mysql.getPassword(),
				"--sink-connect", oracle.getJdbcUrl(), "--sink-user", oracle.getUsername(), "--sink-password",
				oracle.getPassword(), "--sink-staging-schema", oracle.getUsername(), "--mode",
				ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(EXPECTED_ROWS, this.countSinkRows());
	}
}