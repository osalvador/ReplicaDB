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

public class ReplicadbDuckDBFakeContainer {
    private static final Logger LOG = LogManager.getLogger(ReplicadbDuckDBFakeContainer.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String DUCKDB_SINK_FILE = "/sinks/duckdb-sink.sql";
    private static final String DUCKDB_JDBC_URL = "jdbc:duckdb:/tmp/duckdb-replicadb";
    private static final String DUCKDB_DATABASE_NAME = "main";
    private static final String DUCKDB_DRIVER_CLASS = "org.duckdb.DuckDBDriver";

    private static ReplicadbDuckDBFakeContainer container;

    private ReplicadbDuckDBFakeContainer() {}

    public static ReplicadbDuckDBFakeContainer getInstance() {
        if (container == null) {
            container = new ReplicadbDuckDBFakeContainer();
            container.start();
        }
        return container;
    }

    public void start() {
        // Creating Database
        ScriptRunner runner;

        try (Connection con = DriverManager.getConnection(DUCKDB_JDBC_URL)) {
            LOG.info("Creating DuckDB tables");
            runner = new ScriptRunner(con, false, true);
            runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + DUCKDB_SINK_FILE)));

        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getDatabaseName() {
        return DUCKDB_DATABASE_NAME;
    }

    public String getJdbcUrl() {
        return DUCKDB_JDBC_URL;
    }

    public String getDriverClass() {
        return DUCKDB_DRIVER_CLASS;
    }
}
