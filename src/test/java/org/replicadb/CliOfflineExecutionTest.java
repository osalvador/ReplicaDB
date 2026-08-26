package org.replicadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ToolOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CliOfflineExecutionTest {

    @Test
    void executesAnOptionsFileWithSQLiteWithoutMetadataConfiguration(@TempDir Path tempDirectory) throws Exception {
        Path source = tempDirectory.resolve("source.db");
        Path sink = tempDirectory.resolve("sink.db");
        createSource(source);
        createSink(sink);
        Path optionsFile = tempDirectory.resolve("replicadb.properties");
        Files.writeString(optionsFile, String.join(System.lineSeparator(),
                "mode=complete",
                "jobs=1",
                "source.connect=jdbc:sqlite:" + source,
                "source.table=source_items",
                "sink.connect=jdbc:sqlite:" + sink,
                "sink.table=sink_items"));

        ToolOptions options = new ToolOptions(new String[]{"--options-file", optionsFile.toString()});

        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(2, countRows(sink, "sink_items"));
    }

    private static void createSource(Path database) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE source_items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)");
            statement.execute("INSERT INTO source_items (id, payload) VALUES (1, 'one'), (2, 'two')");
        }
    }

    private static void createSink(Path database) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE sink_items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)");
        }
    }

    private static long countRows(Path database, String table) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }
}