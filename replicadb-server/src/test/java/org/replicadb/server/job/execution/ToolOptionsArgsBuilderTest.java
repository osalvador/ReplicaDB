package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.domain.JobDefinition;

import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ToolOptionsArgsBuilderTest {

    private final ToolOptionsArgsBuilder builder = new ToolOptionsArgsBuilder();

    @Test
    void buildsToolOptionsForEachReplicationMode() {
        for (ReplicationMode mode : ReplicationMode.values()) {
            JobDefinition definition = definition(mode,
                    mode == ReplicationMode.INCREMENTAL ? "updated_at" : null,
                    null, null, null);

            assertDoesNotThrow(() -> new ToolOptions(builder.build(definition, null)));
        }
    }

    @Test
    void includesWatermarkColumnAndPrefersPreviousValue() {
        JobDefinition definition = definition(ReplicationMode.INCREMENTAL, "updated_at",
                "100", null, null);

        String[] initialArguments = builder.build(definition, null);
        String[] previousArguments = builder.build(definition, "200");

        assertTrue(Arrays.asList(initialArguments).contains("--incremental-watermark-column"));
        assertTrue(Arrays.asList(initialArguments).contains("100"));
        assertTrue(Arrays.asList(previousArguments).contains("200"));
        assertFalse(Arrays.asList(previousArguments).contains("100"));
    }

    @Test
    void omitsOptionalArgumentsWhenDefinitionValuesAreNull() {
        JobDefinition definition = definition(ReplicationMode.COMPLETE, null, null, null, null);
        String[] arguments = builder.build(definition, null);
        java.util.List<String> argumentList = Arrays.asList(arguments);

        assertFalse(argumentList.contains("--source-user"));
        assertFalse(argumentList.contains("--source-password"));
        assertFalse(argumentList.contains("--source-where"));
        assertFalse(argumentList.contains("--sink-user"));
        assertFalse(argumentList.contains("--sink-password"));
        assertEquals("complete", arguments[argumentList.indexOf("--mode") + 1]);
        assertEquals("2", arguments[argumentList.indexOf("--jobs") + 1]);
    }

    private static JobDefinition definition(ReplicationMode mode, String watermarkColumn,
                                             String initialWatermarkValue, String sourceUser,
                                             String sourceWhere) {
        return new JobDefinition(
                UUID.randomUUID(), "job-" + UUID.randomUUID(), "jdbc:source", sourceUser, null,
                "source_table", sourceWhere, "jdbc:sink", null, null, "sink_table", mode, 2,
                watermarkColumn, initialWatermarkValue, Instant.now(), Instant.now());
    }
}
