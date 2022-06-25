package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MariaDBContainer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbMariaDBContainer extends MariaDBContainer<ReplicadbMariaDBContainer> {
  private static final Logger LOG = LogManager.getLogger(ReplicadbMariaDBContainer.class);
  private static final String IMAGE_VERSION = "mariadb:10.2";
  private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
  private static final String MARIADB_SOURCE_FILE = "/mariadb/mariadb-source.sql";
  private static final String MARIADB_SINK_FILE = "/sinks/mariadb-sink.sql";
  private static ReplicadbMariaDBContainer container;

  private ReplicadbMariaDBContainer () {
    super(IMAGE_VERSION);
  }

  public static ReplicadbMariaDBContainer getInstance() {
    if (container == null) {
      container = new ReplicadbMariaDBContainer().withCommand("--local-infile=1");
      container.start();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();

    // Creating Database
    ScriptRunner runner;
    try (Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      LOG.info("Creating MariaDB tables");
      runner = new ScriptRunner(con, false, true);
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + MARIADB_SINK_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + MARIADB_SOURCE_FILE)));
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void stop() {
    //do nothing, JVM handles shut down
  }
}
