package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MSSQLServerContainer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbSqlserverContainer extends MSSQLServerContainer<ReplicadbSqlserverContainer> {
	private static final Logger LOG = LogManager.getLogger(ReplicadbSqlserverContainer.class);
	private static final String IMAGE_VERSION = "mcr.microsoft.com/mssql/server:2019-latest";
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String SINK_FILE = "/sinks/sqlserver-sink.sql";
	private static final String SOURCE_FILE = "/sqlserver/sqlserver-source.sql";
	private static ReplicadbSqlserverContainer container;

	private ReplicadbSqlserverContainer() {
		super(IMAGE_VERSION);
		this.acceptLicense();
		// Set a strong password that meets SQL Server requirements
		this.withPassword("A_Str0ng_Required_Password");
	}

	public static ReplicadbSqlserverContainer getInstance() {
		if (container == null) {
			container = new ReplicadbSqlserverContainer();
			container.withReuse(true);
			container.start();
		}
		return container;
	}

	@Override
	public void start() {
		super.start();

		// Creating Database
		try (final Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
				container.getPassword())) {
			LOG.info("Creating SQL Server tables for container: {}", this.getClass().getName());
			final ScriptRunner runner = new ScriptRunner(con, false, true);
			runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + SINK_FILE)));
			runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + SOURCE_FILE)));
		} catch (final SQLException | IOException e) {
			LOG.error("Error creating SQL Server tables", e);
			throw new RuntimeException("Failed to initialize SQL Server container tables", e);
		}
	}

	@Override
	public void stop() {
		// do nothing, JVM handles shut down
	}
}
