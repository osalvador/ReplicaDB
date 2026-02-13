package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbSqlserverContainer extends MSSQLServerContainer<ReplicadbSqlserverContainer> {
	private static final Logger LOG = LogManager.getLogger(ReplicadbSqlserverContainer.class);
	// Use Azure SQL Edge for ARM64 compatibility (Apple Silicon Macs)
	// Falls back to SQL Server 2019 on x86_64 architectures
	private static final DockerImageName IMAGE_NAME = selectDockerImage();
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	
	private static DockerImageName selectDockerImage() {
		String arch = System.getProperty("os.arch").toLowerCase();
		String osName = System.getProperty("os.name").toLowerCase();
		
		// Check for ARM64 architecture (native ARM Java)
		if (arch.contains("aarch64") || arch.contains("arm64")) {
			LOG.info("Detected ARM64 architecture, using Azure SQL Edge for compatibility");
			return DockerImageName.parse("mcr.microsoft.com/azure-sql-edge:latest")
					.asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");
		}
		
		// Check for macOS (likely Apple Silicon even if running x86_64 via Rosetta)
		// SQL Server 2019 doesn't support ARM64, so use Azure SQL Edge on Apple Silicon
		if (osName.contains("mac")) {
			try {
				// Check if we're on Apple Silicon using sysctl
				// This works even when Java runs under Rosetta 2 (x86_64 emulation)
				Process process = Runtime.getRuntime().exec(new String[]{"sysctl", "-n", "hw.optional.arm64"});
				BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
				String result = reader.readLine();
				reader.close();
				int exitCode = process.waitFor();
				
				if (exitCode == 0 && "1".equals(result)) {
					LOG.info("Detected Apple Silicon Mac (via sysctl), using Azure SQL Edge for compatibility");
					return DockerImageName.parse("mcr.microsoft.com/azure-sql-edge:latest")
							.asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");
				}
			} catch (IOException | InterruptedException e) {
				LOG.warn("Could not determine system architecture via sysctl, falling back to SQL Server 2019", e);
			}
		}
		
		return DockerImageName.parse("mcr.microsoft.com/mssql/server:2019-latest");
	}
	private static final String SINK_FILE = "/sinks/sqlserver-sink.sql";
	private static final String SOURCE_FILE = "/sqlserver/sqlserver-source.sql";
	private static ReplicadbSqlserverContainer container;

	private ReplicadbSqlserverContainer() {
		super(IMAGE_NAME);
		this.acceptLicense();
		// Set a strong password that meets SQL Server requirements
		this.withPassword("A_Str0ng_Required_Password");
	}

	public static ReplicadbSqlserverContainer getInstance() {
		if (container == null) {
			try {
				LOG.info("Initializing SQL Server container with image: {}", IMAGE_NAME);
				container = new ReplicadbSqlserverContainer();
				container.withReuse(true);
				LOG.info("Starting SQL Server container...");
				container.start();
				LOG.info("SQL Server container started successfully");
			} catch (Exception e) {
				LOG.error("Failed to initialize SQL Server container", e);
				throw new RuntimeException("Failed to initialize SQL Server container", e);
			}
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
