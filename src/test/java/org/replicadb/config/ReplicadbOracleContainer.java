package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.OracleContainer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;

public class ReplicadbOracleContainer extends OracleContainer {
	private static final Logger LOG = LogManager.getLogger(ReplicadbOracleContainer.class);
	// Oracle XE 21c - most stable and modern version
	private static final String IMAGE_VERSION = "gvenzl/oracle-xe:21-slim-faststart";
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String ORACLE_SINK_FILE = "/sinks/oracle-sink.sql";
	private static final String ORACLE_SOURCE_FILE_GEO = "/oracle/oracle-sdo_geometry.sql";
	private static final String ORACLE_SOURCE_FILE = "/oracle/oracle-source.sql";
	private static ReplicadbOracleContainer container;

	private ReplicadbOracleContainer() {
		super(IMAGE_VERSION);
	}

	public static ReplicadbOracleContainer getInstance() {
		if (container == null) {
			container = new ReplicadbOracleContainer();
			// Improved configuration for better reliability
			container.withDatabaseName("xe").withUsername("test").withPassword("test").withReuse(true)
					.withStartupTimeoutSeconds(240).withConnectTimeoutSeconds(120).withSharedMemorySize(1073741824L); // 1GB in bytes
																												// adequate
																												// memory
			container.start();
		}
		return container;
	}

	@Override
	public void start() {
		super.start();

		// Creating Database
		final ScriptRunner runner;
		// Set the Timezone, this is required to start the container in GitHub Actions
		final TimeZone timeZone = TimeZone.getTimeZone("UTC");
		TimeZone.setDefault(timeZone);
		try (final Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
				container.getPassword())) {
			LOG.info("Creating Oracle tables");
			runner = new ScriptRunner(con, false, true);
			runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SINK_FILE)));
			runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SOURCE_FILE)));
			runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SOURCE_FILE_GEO)));
		} catch (final SQLException | IOException e) {
			// throw new RuntimeException(e);
		}

	}

	@Override
	public void stop() {
		// do nothing, JVM handles shut down
	}
}
