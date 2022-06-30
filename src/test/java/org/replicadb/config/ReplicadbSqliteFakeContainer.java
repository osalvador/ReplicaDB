package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbSqliteFakeContainer {
  private static final Logger LOG = LogManager.getLogger(ReplicadbSqliteFakeContainer.class);
  private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
  private static final String SQLITE_SINK_FILE = "/sinks/sqlite-sink.sql";
  private static final String SQLITE_SOURCE_FILE = "/sqlite/sqlite-source.sql";
  private static final String SQLITE_JDBC_URL="jdbc:sqlite:/tmp/sqlite-replicadb";
  private static final String SQLITE_DATABASE_NAME ="main";

  private static ReplicadbSqliteFakeContainer container;

  private ReplicadbSqliteFakeContainer () {}

  public static ReplicadbSqliteFakeContainer getInstance() {
    if (container == null) {
      container = new ReplicadbSqliteFakeContainer();
      container.start();
    }
    return container;
  }


  public void start() {
    // Creating Database
    ScriptRunner runner;

    try (Connection con = DriverManager.getConnection(SQLITE_JDBC_URL)) {
      LOG.info("Creating Sqlite tables");
      runner = new ScriptRunner(con, false, true);
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + SQLITE_SINK_FILE)));
      runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + SQLITE_SOURCE_FILE)));

    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }

  }

  public String getDatabaseName(){
    return SQLITE_DATABASE_NAME;
  }
  public String getJdbcUrl(){
    return SQLITE_JDBC_URL;
  }

}
