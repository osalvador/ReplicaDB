package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.TimeZone;

public class ReplicadbOracleContainer extends OracleContainer {
	private static final Logger LOG = LogManager.getLogger(ReplicadbOracleContainer.class);
	// Using Oracle Free image with compatibility declaration for ARM architecture
	private static final DockerImageName ORACLE_FREE_IMAGE = DockerImageName
			.parse("gvenzl/oracle-free:23-slim-faststart").asCompatibleSubstituteFor("gvenzl/oracle-xe");
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String ORACLE_SINK_FILE = "/sinks/oracle-sink.sql";
	private static final String ORACLE_SOURCE_FILE_GEO = "/oracle/oracle-sdo_geometry.sql";
	private static final String ORACLE_SOURCE_FILE = "/oracle/oracle-source.sql";
	private static ReplicadbOracleContainer container;
	
	// Track whether geometry support is available
	private boolean geometryTablesCreated = false;
	// Track whether XML support is available
	private boolean xmlSupportAvailable = false;

	private ReplicadbOracleContainer() {
		super(ORACLE_FREE_IMAGE);
	}

	public static ReplicadbOracleContainer getInstance() {
		if (container == null) {
			container = new ReplicadbOracleContainer();
			// Enhanced configuration for Oracle Free (ARM-compatible)
			container // Oracle Free uses FREEPDB1 as default PDB
					.withUsername("test").withPassword("test").withReuse(false) // Disable reuse to avoid stale
																				// container issues
					.withStartupTimeout(Duration.ofMinutes(10)) // Increased to 10 minutes for PMON startup
					.withSharedMemorySize(3221225472L) // Increased to 3GB shared memory
					// .withEnv("ORACLE_INIT_PARAMS", "processes=200,sessions=300,memory_target=2G")
					// // Increase limits
					.withPrivilegedMode(true); // Enable privileged mode for Oracle processes

			container.start();
		}
		return container;
	}

	@Override
	public void start() {
		super.start();

		// Set timezone for consistency
		final TimeZone timeZone = TimeZone.getTimeZone("UTC");
		TimeZone.setDefault(timeZone);

		// Creating Database with better error handling
		try (final Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
				container.getPassword())) {
			LOG.info("Creating Oracle tables for database: {}", container.getDatabaseName());
			final ScriptRunner runner = new ScriptRunner(con, false, true);

			// Run scripts with individual error handling
			try {
				runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SINK_FILE)));
				LOG.info("Oracle sink table created successfully");
			} catch (final IOException e) {
				LOG.error("Failed to create Oracle sink table: {}", e.getMessage());
				throw new RuntimeException("Oracle sink table creation failed", e);
			}

			try {
				runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SOURCE_FILE)));
				LOG.info("Oracle source table created successfully");
				
				// Test if XML columns work by querying the XML column
				try (final Statement testStmt = con.createStatement()) {
					final ResultSet rs = testStmt.executeQuery("SELECT c_xml FROM t_source WHERE ROWNUM <= 1");
					if (rs.next()) {
						rs.getObject(1); // Try to access XML data
						xmlSupportAvailable = true;
						LOG.info("Oracle XML support confirmed");
					}
				} catch (Exception xmlEx) {
					LOG.warn("Oracle XML support not available (may be limited in Oracle Free): {}", xmlEx.getMessage());
					xmlSupportAvailable = false;
				}
			} catch (final IOException e) {
				LOG.error("Failed to create Oracle source table: {}", e.getMessage());
				throw new RuntimeException("Oracle source table creation failed", e);
			}

			try {
				runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SOURCE_FILE_GEO)));
				LOG.info("Oracle geometry table created successfully");
				geometryTablesCreated = true;
			} catch (final Exception e) {
				LOG.warn("Failed to create Oracle geometry table (SDO_GEOMETRY not supported in Oracle Free): {}", e.getMessage());
				geometryTablesCreated = false;
				// Don't throw exception for geometry table as it's optional
			}

		} catch (final SQLException e) {
			LOG.error("Failed to connect to Oracle database: {}", e.getMessage());
			throw new RuntimeException("Oracle database connection failed", e);
		}
	}

	/**
	 * Check if XMLType support is available.
	 * Oracle Free edition may have limitations with XMLType columns.
	 * 
	 * @return true if XML operations work correctly, false otherwise
	 */
	public boolean isXmlSupportAvailable() {
		return xmlSupportAvailable;
	}
	
	/**
	 * Check if SDO_GEOMETRY support is available.
	 * Oracle Free edition doesn't support spatial data types.
	 * 
	 * @return true if geometry tables were created successfully, false otherwise
	 */
	public boolean isGeometrySupportAvailable() {
		return geometryTablesCreated;
	}
	
	/**
	 * Check if this is Oracle Free edition based on the Docker image.
	 * 
	 * @return true if using Oracle Free edition
	 */
	public boolean isOracleFreeEdition() {
		return getDockerImageName().toString().contains("oracle-free");
	}

	@Override
	public void stop() {
		// Allow proper container cleanup
		super.stop();
	}
}
