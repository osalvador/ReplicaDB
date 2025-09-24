package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Postgres2CsvFileTest {
	private static final Logger LOG = LogManager.getLogger(Postgres2CsvFileTest.class);
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
	private static final int TOTAL_SINK_ROWS = 4097;

	private static final String SINK_FILE_PATH = "file:///tmp/fileSink.csv";
	private static final String SINK_FILE_URI_PATH = "file:///tmp/fileSink.csv";

	private Connection postgresConn;

	@Container
	public static final PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer
			.getInstance();

	@BeforeAll
	static void setUp() {
		// Container auto-starts with @Container annotation
	}

	@BeforeEach
	void before() throws SQLException {
		this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
				postgres.getPassword());
	}

	@AfterEach
	void tearDown() throws SQLException {
		final File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
		LOG.info("Deleted file: {}", sinkFile.delete());
		this.postgresConn.close();

		// Clean the static tempFiles HashMap.
		FileManager.setTempFilesPath(new HashMap<>());
	}

	public int countSinkRows() throws IOException {
		final Path path = Paths.get(URI.create(SINK_FILE_URI_PATH));
		try (final var lines = Files.lines(path)) {
			final int count = (int) lines.count();
			LOG.info("File total Rows: {}", count);
			return count;
		}
	}

	@Test
	void testPostgresConnection() throws SQLException {
		try (final Statement stmt = this.postgresConn.createStatement();
				final ResultSet rs = stmt.executeQuery("SELECT count(*) from t_source")) {
			rs.next();
			final int totalRows = rs.getInt(1);
			assertEquals(TOTAL_SINK_ROWS, totalRows);
		}
	}

	@Test
	void testFileComplete() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", SINK_FILE_PATH, "--sink-file-format",
				FileFormats.CSV.getType()};
		final ToolOptions options = new ToolOptions(args);

		final Properties sinkConnectionParams = new Properties();
		sinkConnectionParams.setProperty("format", "EXCEL");
		options.setSinkConnectionParams(sinkConnectionParams);

		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testFileCompleteParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", SINK_FILE_PATH, "--sink-file-format",
				FileFormats.CSV.getType(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);

		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testFileIncremental() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", SINK_FILE_PATH, "--sink-file-format",
				FileFormats.CSV.getType(), "--mode", ReplicationMode.INCREMENTAL.getModeText()};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

	@Test
	void testFileIncrementalParallel() throws ParseException, IOException {
		final String[] args = {"--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE, "--source-connect",
				postgres.getJdbcUrl(), "--source-user", postgres.getUsername(), "--source-password",
				postgres.getPassword(), "--sink-connect", SINK_FILE_PATH, "--sink-file-format",
				FileFormats.CSV.getType(), "--mode", ReplicationMode.INCREMENTAL.getModeText(), "--jobs", "4"};
		final ToolOptions options = new ToolOptions(args);
		Assertions.assertEquals(0, ReplicaDB.processReplica(options));
		assertEquals(TOTAL_SINK_ROWS, this.countSinkRows());
	}

}