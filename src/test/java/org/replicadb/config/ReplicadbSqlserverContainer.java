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
  private static final String IMAGE_VERSION = "mcr.microsoft.com/mssql/server:2017-CU12";
  private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
  private static final String SINK_FILE = "/sinks/sqlserver-sink.sql";
  private static final String SOURCE_FILE = "/sqlserver/sqlserver-source.sql";
  private static ReplicadbSqlserverContainer container;

  private ReplicadbSqlserverContainer () {
    super(IMAGE_VERSION);
  }

  public static ReplicadbSqlserverContainer getInstance() {
    if (container == null) {
      container = new ReplicadbSqlserverContainer();
      container.acceptLicense();
      container.withReuse(true);
      container.start();
    }
    return container;
  }

  @Override
  public String getDatabaseName () {
    return "dbo";
  }

  @Override
  public void start() {
    super.start();

    // Creating Database
    try (Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      LOG.info("Creating tables for container: {} ", this.getClass().getName());
      ScriptRunner runner = new ScriptRunner(con, false, true);
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + SINK_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + SOURCE_FILE)));
    } catch (SQLException | IOException e) {
      //throw new RuntimeException(e);
    }

  }

  @Override
  public void stop() {
    //do nothing, JVM handles shut down
  }
}
