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
  private static final String IMAGE_VERSION = "gvenzl/oracle-xe:11";
  private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
  private static final String ORACLE_SINK_FILE = "/sinks/oracle-sink.sql";
  private static final String ORACLE_SOURCE_FILE_GEO = "/oracle/oracle-sdo_geometry.sql";
  private static final String ORACLE_SOURCE_FILE = "/oracle/oracle-source.sql";
  private static ReplicadbOracleContainer container;

  private ReplicadbOracleContainer () {
    super(IMAGE_VERSION);
  }

  public static ReplicadbOracleContainer getInstance() {
    if (container == null) {
      container = new ReplicadbOracleContainer();      
      container.usingSid();
      container.withReuse(true);
      container.start();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();

    // Creating Database
    ScriptRunner runner;
    // Set the Timezone, this is required to start the container in GitHub Actions
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    TimeZone.setDefault(timeZone);
    try (Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      LOG.info("Creating Oracle tables");
      runner = new ScriptRunner(con, false, true);
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SINK_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SOURCE_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + ORACLE_SOURCE_FILE_GEO)));
    } catch (SQLException | IOException e) {
      //throw new RuntimeException(e);
    }

  }

  @Override
  public void stop() {
    //do nothing, JVM handles shut down
  }
}
