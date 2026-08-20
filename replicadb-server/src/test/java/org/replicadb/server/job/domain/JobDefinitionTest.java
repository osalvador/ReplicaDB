package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobDefinitionTest {

    @Test
    void acceptsAValidDefinitionWithoutPasswords() {
        assertDoesNotThrow(() -> definition(ReplicationMode.COMPLETE, null, null, null));
    }

    @Test
    void acceptsEnvironmentPasswordReferences() {
        assertDoesNotThrow(() -> definition(ReplicationMode.COMPLETE,
                null, "${env:SOURCE_PASSWORD}", "${env:SINK_PASSWORD}"));
    }

        @Test
        void appliesModeSpecificRetryDefaults() {
                JobDefinition complete = definition(ReplicationMode.COMPLETE, null, null, null);
                JobDefinition atomic = definition(ReplicationMode.COMPLETE_ATOMIC, null, null, null);
                JobDefinition incremental = definition(ReplicationMode.INCREMENTAL, "updated_at", null, null);

                assertEquals(3, complete.maxAttempts());
                assertEquals(60, complete.retryBackoffSeconds());
                assertEquals(false, complete.automaticRetryEnabled());
                assertEquals(true, atomic.automaticRetryEnabled());
                assertEquals(true, incremental.automaticRetryEnabled());
        }

        @Test
        void acceptsExplicitCompleteModeRetryOptIn() {
                JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
                                .withMode(ReplicationMode.COMPLETE)
                                .withRetryPolicy(new RetryPolicy(5, 120, true))
                                .build();

                assertEquals(5, definition.maxAttempts());
                assertEquals(120, definition.retryBackoffSeconds());
                assertEquals(true, definition.automaticRetryEnabled());
        }

        @Test
        void rejectsNullRetryPolicy() {
                assertThrows(NullPointerException.class,
                                () -> definitionWithExecution(100, 0, null));
        }

    @Test
    void rejectsBlankRequiredFields() {
        assertThrows(IllegalArgumentException.class, () -> definition(
                ReplicationMode.COMPLETE, "", null, null, "job"));
        assertThrows(IllegalArgumentException.class, () -> definition(
                ReplicationMode.COMPLETE, "source_watermark", null, null, " ", "job"));
        assertThrows(IllegalArgumentException.class, () -> definition(
                ReplicationMode.COMPLETE, "source_watermark", null, null, "source_table", ""));
    }

    @Test
    void rejectsNonPositiveJobs() {
        assertThrows(IllegalArgumentException.class, () -> new JobDefinition(
                null, "job", "jdbc:source", null, null, "source_table", null,
                "jdbc:sink", null, null, "sink_table", ReplicationMode.COMPLETE, 0,
                null, null, null, null));
    }

        @Test
        void rejectsNonPositiveFetchSize() {
                assertThrows(IllegalArgumentException.class, () -> definitionWithExecution(0, 0));
        }

        @Test
        void rejectsNegativeBandwidthThrottling() {
                assertThrows(IllegalArgumentException.class, () -> definitionWithExecution(100, -1));
        }

    @Test
    void rejectsWatermarkColumnOutsideIncrementalMode() {
        assertThrows(IllegalArgumentException.class,
                () -> definition(ReplicationMode.COMPLETE, "source_watermark", null, null));
        assertThrows(IllegalArgumentException.class,
                () -> definition(ReplicationMode.COMPLETE_ATOMIC, "source_watermark", null, null));
    }

    @Test
    void rejectsLiteralSourcePassword() {
        assertThrows(IllegalArgumentException.class,
                () -> definition(ReplicationMode.COMPLETE, null, "literal-password", null));
    }

    @Test
    void rejectsLiteralSinkPassword() {
        assertThrows(IllegalArgumentException.class,
                () -> definition(ReplicationMode.COMPLETE, null, null, "literal-password"));
    }

    @Test
    void rejectsEmbeddedCredentialsInConnectionStrings() {
        assertThrows(IllegalArgumentException.class, () -> new JobDefinition(
                null, "job", "jdbc:postgresql://user:password@source", null, null, "source_table", null,
                "jdbc:sink", null, null, "sink_table", ReplicationMode.COMPLETE, 1,
                null, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new JobDefinition(
                null, "job", "jdbc:source", null, null, "source_table", null,
                "jdbc:postgresql://sink?password=literal", null, null, "sink_table",
                ReplicationMode.COMPLETE, 1, null, null, null, null));
    }

    private static JobDefinition definition(ReplicationMode mode, String watermarkColumn,
                                             String sourcePassword, String sinkPassword) {
        return definition(mode, watermarkColumn, sourcePassword, sinkPassword, "job", "source_table");
    }

    private static JobDefinition definition(ReplicationMode mode, String watermarkColumn,
                                             String sourcePassword, String sinkPassword,
                                             String name) {
        return definition(mode, watermarkColumn, sourcePassword, sinkPassword, name, "source_table");
    }

    private static JobDefinition definition(ReplicationMode mode, String watermarkColumn,
                                             String sourcePassword, String sinkPassword,
                                             String sourceTable, String name) {
        return new JobDefinition(
                null, name, "jdbc:source", null, sourcePassword, sourceTable, null,
                "jdbc:sink", null, sinkPassword, "sink_table", mode, 2,
                watermarkColumn, null, Instant.now(), Instant.now());
    }

        private static JobDefinition definitionWithExecution(int fetchSize, int bandwidthThrottling) {
                return definitionWithExecution(fetchSize, bandwidthThrottling,
                        RetryPolicy.defaultsFor(ReplicationMode.COMPLETE));
        }

        private static JobDefinition definitionWithExecution(int fetchSize, int bandwidthThrottling,
                                                             RetryPolicy retryPolicy) {
                ConnectionCredentials sourceConnection = new ConnectionCredentials("jdbc:source", null, null, null, null);
                ConnectionCredentials sinkConnection = new ConnectionCredentials("jdbc:sink", null, null, null, null);
                return new JobDefinition(
                                null, "job",
                                new SourceEndpoint(sourceConnection, "source_table", null, null, null),
                                new SinkEndpoint(sinkConnection, "sink_table", null, null, false, false),
                                ReplicationMode.COMPLETE, 1, null, null, Instant.now(), Instant.now(),
                                fetchSize, bandwidthThrottling, false, retryPolicy);
        }
}
