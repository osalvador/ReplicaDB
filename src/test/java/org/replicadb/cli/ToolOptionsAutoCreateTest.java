package org.replicadb.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for --sink-auto-create option parsing in ToolOptions.
 * Tests verify that the option is correctly parsed from command line arguments
 * and properties files.
 */
class ToolOptionsAutoCreateTest {

    @Test
    void testSinkAutoCreate_defaultIsFalse() throws Exception {
        // Given: Basic required options without --sink-auto-create
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table"
        };

        // When: Options are parsed
        ToolOptions options = new ToolOptions(args);

        // Then: sinkAutoCreate should default to false
        assertFalse(options.isSinkAutoCreate(), "sinkAutoCreate should default to false");
    }

    @Test
    void testSinkAutoCreate_parsedFromCLI() throws Exception {
        // Given: Command line with --sink-auto-create flag
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table",
            "--sink-auto-create"
        };

        // When: Options are parsed
        ToolOptions options = new ToolOptions(args);

        // Then: sinkAutoCreate should be true
        assertTrue(options.isSinkAutoCreate(), "--sink-auto-create flag should set sinkAutoCreate to true");
    }

    @Test
    void testSinkAutoCreate_parsedFromPropertiesFileTrue(@TempDir Path tempDir) throws Exception {
        // Given: Properties file with sink.auto.create=true
        Path propsFile = tempDir.resolve("test.properties");
        try (FileWriter writer = new FileWriter(propsFile.toFile())) {
            writer.write("source.connect=jdbc:postgresql://localhost:5432/source\n");
            writer.write("source.table=source_table\n");
            writer.write("sink.connect=jdbc:postgresql://localhost:5432/sink\n");
            writer.write("sink.table=sink_table\n");
            writer.write("sink.auto.create=true\n");
        }

        String[] args = {
            "--options-file", propsFile.toString()
        };

        // When: Options are loaded from properties file
        ToolOptions options = new ToolOptions(args);

        // Then: sinkAutoCreate should be true
        assertTrue(options.isSinkAutoCreate(), "sink.auto.create=true in properties file should set sinkAutoCreate to true");
    }

    @Test
    void testSinkAutoCreate_parsedFromPropertiesFileFalse(@TempDir Path tempDir) throws Exception {
        // Given: Properties file with sink.auto.create=false
        Path propsFile = tempDir.resolve("test.properties");
        try (FileWriter writer = new FileWriter(propsFile.toFile())) {
            writer.write("source.connect=jdbc:postgresql://localhost:5432/source\n");
            writer.write("source.table=source_table\n");
            writer.write("sink.connect=jdbc:postgresql://localhost:5432/sink\n");
            writer.write("sink.table=sink_table\n");
            writer.write("sink.auto.create=false\n");
        }

        String[] args = {
            "--options-file", propsFile.toString()
        };

        // When: Options are loaded from properties file
        ToolOptions options = new ToolOptions(args);

        // Then: sinkAutoCreate should be false
        assertFalse(options.isSinkAutoCreate(), "sink.auto.create=false in properties file should set sinkAutoCreate to false");
    }

    @Test
    void testSinkAutoCreate_CLIOverridesPropertiesFile(@TempDir Path tempDir) throws Exception {
        // Given: Properties file with sink.auto.create=false, but CLI has --sink-auto-create
        Path propsFile = tempDir.resolve("test.properties");
        try (FileWriter writer = new FileWriter(propsFile.toFile())) {
            writer.write("source.connect=jdbc:postgresql://localhost:5432/source\n");
            writer.write("source.table=source_table\n");
            writer.write("sink.connect=jdbc:postgresql://localhost:5432/sink\n");
            writer.write("sink.table=sink_table\n");
            writer.write("sink.auto.create=false\n");
        }

        String[] args = {
            "--options-file", propsFile.toString(),
            "--sink-auto-create"
        };

        // When: Options are loaded with CLI override
        ToolOptions options = new ToolOptions(args);

        // Then: CLI flag should override properties file
        assertTrue(options.isSinkAutoCreate(), "CLI --sink-auto-create should override properties file");
    }

    @Test
    void testSinkAutoCreate_setterWorks() throws Exception {
        // Given: Basic options
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table"
        };

        ToolOptions options = new ToolOptions(args);

        // When: setSinkAutoCreate is called
        options.setSinkAutoCreate(true);

        // Then: Getter should return the set value
        assertTrue(options.isSinkAutoCreate(), "setSinkAutoCreate(true) should set the value");

        // When: Set to false
        options.setSinkAutoCreate(false);

        // Then: Should be false
        assertFalse(options.isSinkAutoCreate(), "setSinkAutoCreate(false) should set the value");
    }

    @Test
    void testSinkAutoCreate_sourceColumnDescriptors_defaultIsNull() throws Exception {
        // Given: Basic options
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table"
        };

        // When: Options are created
        ToolOptions options = new ToolOptions(args);

        // Then: sourceColumnDescriptors should be null by default
        assertNull(options.getSourceColumnDescriptors(), "sourceColumnDescriptors should default to null");
    }

    @Test
    void testSinkAutoCreate_sourcePrimaryKeys_defaultIsNull() throws Exception {
        // Given: Basic options
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table"
        };

        // When: Options are created
        ToolOptions options = new ToolOptions(args);

        // Then: sourcePrimaryKeys should be null by default
        assertNull(options.getSourcePrimaryKeys(), "sourcePrimaryKeys should default to null");
    }
}
