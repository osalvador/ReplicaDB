package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbPostgresqlContainer extends PostgreSQLContainer<ReplicadbPostgresqlContainer> {
  private static final Logger LOG = LogManager.getLogger(ReplicadbPostgresqlContainer.class);
  private static final String IMAGE_VERSION = "postgres:9.6";
  private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
  private static final String POSTGRES_SINK_FILE = "/sinks/pg-sink.sql";
  private static final String POSTGRES_SOURCE_FILE = "/postgres/pg-source.sql";
  private static ReplicadbPostgresqlContainer container;

  private ReplicadbPostgresqlContainer () {
    super(IMAGE_VERSION);
  }

  public static ReplicadbPostgresqlContainer getInstance() {
    if (container == null) {
      container = new ReplicadbPostgresqlContainer();
      container.start();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();

    // Creating Database
    try (Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      LOG.info("Creating Postgres tables");
      ScriptRunner runner = new ScriptRunner(con, false, true);
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + POSTGRES_SINK_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + POSTGRES_SOURCE_FILE)));
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void stop() {
    //do nothing, JVM handles shut down
  }
}
