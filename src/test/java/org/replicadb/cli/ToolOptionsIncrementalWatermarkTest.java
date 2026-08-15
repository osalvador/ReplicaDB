package org.replicadb.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ToolOptionsIncrementalWatermarkTest {

    @Test
    void parsesColumnAndValueAndRoundTripsViaGetters() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--sink-connect", "jdbc:postgresql://sink",
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--incremental-watermark-column", "c_integer",
                "--incremental-watermark-value", "42"
        });

        assertEquals("c_integer", options.getIncrementalWatermarkColumn());
        assertEquals("42", options.getIncrementalWatermarkValue());
    }

    @Test
    void columnWithoutValueIsValidForFirstRun() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--sink-connect", "jdbc:postgresql://sink",
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--incremental-watermark-column", "c_integer"
        });

        assertEquals("c_integer", options.getIncrementalWatermarkColumn());
        assertNull(options.getIncrementalWatermarkValue());
    }

    @Test
    void valueWithoutColumnThrows() {
        assertThrows(IllegalArgumentException.class, () -> new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--sink-connect", "jdbc:postgresql://sink",
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--incremental-watermark-value", "42"
        }));
    }

    @Test
    void columnWithCompleteModeThrows() {
        assertThrows(IllegalArgumentException.class, () -> new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--sink-connect", "jdbc:postgresql://sink",
                "--mode", ReplicationMode.COMPLETE.getModeText(),
                "--incremental-watermark-column", "c_integer"
        }));
    }

    @Test
    void columnWithReplicationTableEntriesThrows(@TempDir Path tempDir) throws IOException {
        Path optionsFile = tempDir.resolve("multi-table.properties");
        Files.writeString(optionsFile, String.join(System.lineSeparator(),
                "mode=" + ReplicationMode.INCREMENTAL.getModeText(),
                "source.connect=jdbc:postgresql://source",
                "sink.connect=jdbc:postgresql://sink",
                "incremental.watermark.column=c_integer",
                "replication.table.1.source=customers",
                "replication.table.1.sink=customer_copy"));

        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptions(new String[]{"--options-file", optionsFile.toString()}));
    }
}
