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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class ReplicadbOracleContainer extends OracleContainer {
	private static final Logger LOG = LogManager.getLogger(ReplicadbOracleContainer.class);
	
	// Oracle version to Docker image mapping for cross-version testing
	private static final Map<String, String> VERSION_IMAGES = new HashMap<>();
	static {
		VERSION_IMAGES.put("11", "gvenzl/oracle-xe:11-slim-faststart");
		VERSION_IMAGES.put("18", "gvenzl/oracle-xe:18-slim-faststart");
		VERSION_IMAGES.put("21", "gvenzl/oracle-xe:21-slim-faststart");
		VERSION_IMAGES.put("23", "gvenzl/oracle-free:23-slim-faststart");
	}
	
	// Using Oracle Free image with compatibility declaration for ARM architecture
	private static final DockerImageName ORACLE_FREE_IMAGE = DockerImageName
			.parse("gvenzl/oracle-free:23-slim-faststart").asCompatibleSubstituteFor("gvenzl/oracle-xe");
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String ORACLE_SINK_FILE = "/sinks/oracle-sink.sql";
	private static final String ORACLE_SOURCE_FILE_GEO = "/oracle/oracle-sdo_geometry.sql";
	private static final String ORACLE_SOURCE_FILE = "/oracle/oracle-source.sql";
	private static ReplicadbOracleContainer container;
	
	// Per-version container instances for cross-version testing
	private static final Map<String, ReplicadbOracleContainer> versionContainers = new HashMap<>();
	
	// Track whether geometry support is available
	private boolean geometryTablesCreated = false;
	// Track whether XML support is available
	private boolean xmlSupportAvailable = false;
	// Oracle major version for this container
	private final String oracleVersion;

	private ReplicadbOracleContainer() {
		super(ORACLE_FREE_IMAGE);
		this.oracleVersion = "23";
	}
	
	private ReplicadbOracleContainer(String version) {
		super(getDockerImageForVersion(version));
		this.oracleVersion = version;
	}
	
	private static DockerImageName getDockerImageForVersion(String version) {
		String imageName = VERSION_IMAGES.get(version);
		if (imageName == null) {
			throw new IllegalArgumentException("Unsupported Oracle version: " + version + 
				". Supported versions: " + VERSION_IMAGES.keySet());
		}
		// All gvenzl images need compatibility declaration for testcontainers oracle module
		// The OracleContainer class expects gvenzl/oracle-free as the base image
		return DockerImageName.parse(imageName).asCompatibleSubstituteFor("gvenzl/oracle-free");
	}

	public static ReplicadbOracleContainer getInstance() {
		if (container == null) {
			container = new ReplicadbOracleContainer();
			configureContainer(container);
			container.start();
		}
		return container;
	}
	
	/**
	 * Get an Oracle container instance for a specific version.
	 * Useful for cross-version LOB replication testing (e.g., 11g to 23c).
	 * 
	 * @param version Oracle version string: "11", "18", "21", "23"
	 * @return Container configured for the specified Oracle version
	 */
	public static ReplicadbOracleContainer getInstance(String version) {
		return versionContainers.computeIfAbsent(version, v -> {
			ReplicadbOracleContainer versionContainer = new ReplicadbOracleContainer(v);
			configureContainer(versionContainer);
			versionContainer.start();
			return versionContainer;
		});
	}
	
	/**
	 * Get an Oracle 11g container instance for cross-version testing.
	 * Note: May not be available on ARM architectures.
	 */
	public static ReplicadbOracleContainer getOracle11gInstance() {
		return getInstance("11");
	}
	
	/**
	 * Get an Oracle 18c container instance for cross-version testing.
	 */
	public static ReplicadbOracleContainer getOracle18cInstance() {
		return getInstance("18");
	}
	
	/**
	 * Get an Oracle 21c container instance for cross-version testing.
	 */
	public static ReplicadbOracleContainer getOracle21cInstance() {
		return getInstance("21");
	}
	
	/**
	 * Get an Oracle 23c (Free) container instance.
	 */
	public static ReplicadbOracleContainer getOracle23cInstance() {
		return getInstance("23");
	}
	
	private static void configureContainer(ReplicadbOracleContainer container) {
		// Enhanced configuration for Oracle containers
		container
			.withUsername("test")
			.withPassword("test")
			.withReuse(false)
			.withStartupTimeout(Duration.ofMinutes(10))
			.withSharedMemorySize(3221225472L)
			.withPrivilegedMode(true);
	}
	
	/**
	 * Get the Oracle major version number for this container.
	 * 
	 * @return Major version string (e.g., "11", "18", "21", "23")
	 */
	public String getOracleVersion() {
		return oracleVersion;
	}
	
	/**
	 * Get the Oracle major version as an integer.
	 * 
	 * @return Major version number
	 */
	public int getOracleMajorVersion() {
		return Integer.parseInt(oracleVersion);
	}

	@Override
	public void start() {
		super.start();

		// Set timezone for consistency
		final TimeZone timeZone = TimeZone.getTimeZone("UTC");
		TimeZone.setDefault(timeZone);

		// Creating Database with better error handling
		try (final Connection con = DriverManager.getConnection(this.getJdbcUrl(), this.getUsername(),
				this.getPassword())) {
			LOG.info("Creating Oracle tables for database: {}", this.getDatabaseName());
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
