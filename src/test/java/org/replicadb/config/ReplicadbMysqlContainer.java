package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MySQLContainer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbMysqlContainer extends MySQLContainer<ReplicadbMysqlContainer> {
  private static final Logger LOG = LogManager.getLogger(ReplicadbMysqlContainer.class);
  private static final String IMAGE_VERSION = "mysql:5.6";
  private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
  private static final String MYSQL_SOURCE_FILE = "/mysql/mysql-source.sql";
  private static final String MYSQL_SINK_FILE = "/sinks/mysql-sink.sql";
  private static ReplicadbMysqlContainer container;

  private ReplicadbMysqlContainer () {
    super(IMAGE_VERSION);
  }

  public static ReplicadbMysqlContainer getInstance() {
    if (container == null) {
      container = new ReplicadbMysqlContainer()
          .withCommand("--local-infile=1");
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
      LOG.info("Creating MySQL tables");
      runner = new ScriptRunner(con, false, true);
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + MYSQL_SINK_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + MYSQL_SOURCE_FILE)));
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void stop() {
    //do nothing, JVM handles shut down
  }
}
