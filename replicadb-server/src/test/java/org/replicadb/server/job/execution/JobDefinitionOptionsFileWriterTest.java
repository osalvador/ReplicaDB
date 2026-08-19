package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.OptionsFile;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ConnectionCredentials;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.domain.StagingOptions;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobDefinitionOptionsFileWriterTest {

    private final JobDefinitionOptionsFileWriter writer = new JobDefinitionOptionsFileWriter();

    @Test
    void writesAllOptionsAndConnectionParameters() throws Exception {
        JobDefinition definition = definition();
        Path path = writer.write(definition, "200", JobDefinitionOptionsFileWriterTest::resolveSourcePassword);
        try {
            Properties properties = new Properties();
            try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                properties.load(reader);
            }

            assertEquals("incremental", properties.getProperty("mode"));
            assertEquals("3", properties.getProperty("jobs"));
            assertEquals("250", properties.getProperty("fetch.size"));
            assertEquals("512", properties.getProperty("bandwidth.throttling"));
            assertEquals("true", properties.getProperty("verbose"));
            assertEquals("resolved-value", properties.getProperty("source.password"));
            assertEquals("ActiveDirectoryDefault", properties.getProperty("source.auth.mode"));
            assertEquals("source-client", properties.getProperty("source.auth.principal.id"));
            assertEquals("id, name", properties.getProperty("source.columns"));
            assertEquals("select id, name from source_table", properties.getProperty("source.query"));
            assertEquals("require", properties.getProperty("source.connect.parameter.sslmode"));
            assertEquals("staging", properties.getProperty("sink.staging.schema"));
            assertEquals("sink_stage", properties.getProperty("sink.staging.table"));
            assertEquals("true", properties.getProperty("sink.disable.escape"));
            assertEquals("true", properties.getProperty("sink.disable.truncate"));
            assertEquals("100", properties.getProperty("sink.connect.parameter.batch.size"));
            assertEquals("200", properties.getProperty("incremental.watermark.value"));

            OptionsFile optionsFile = new OptionsFile(path.toString());
            assertEquals("require", optionsFile.getSourceConnectionParams().getProperty("sslmode"));
            assertEquals("100", optionsFile.getSinkConnectionParams().getProperty("batch.size"));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    void omitsOptionalValuesAndUsesInitialWatermarkWhenNoPreviousValue() throws Exception {
        JobDefinition definition = new JobDefinition(
                UUID.randomUUID(), "complete-job", "jdbc:source", null, null, "source_table", null,
                "jdbc:sink", null, null, "sink_table", ReplicationMode.COMPLETE, 2,
                null, null, Instant.now(), Instant.now());
        Path path = writer.write(definition, null, Function.identity());
        try {
            Properties properties = new Properties();
            try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                properties.load(reader);
            }

            assertFalse(properties.containsKey("source.user"));
            assertFalse(properties.containsKey("source.password"));
            assertFalse(properties.containsKey("source.where"));
            assertFalse(properties.containsKey("source.query"));
            assertFalse(properties.containsKey("incremental.watermark.column"));
            assertEquals("false", properties.getProperty("verbose"));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    void createsOwnerOnlyFilePermissionsWhenSupported() throws Exception {
        Path path = writer.write(definition(), null, Function.identity());
        try {
            PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            if (view != null) {
                assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(path)));
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static JobDefinition definition() {
        return new JobDefinition(
                UUID.randomUUID(), "advanced-job",
                new SourceEndpoint(
                        new ConnectionCredentials("jdbc:source", "source-user", "${env:SOURCE_PASSWORD}",
                                new AzureAuthentication("ActiveDirectoryDefault", "source-client", "source-login",
                                        "source-cert", "source-key"),
                                Map.of("sslmode", "require")),
                        null, "id, name", "id > 10", "select id, name from source_table"),
                new SinkEndpoint(
                        new ConnectionCredentials("jdbc:sink", "sink-user", null,
                                new AzureAuthentication(null, null, null, null, null),
                                Map.of("batch.size", "100")),
                        "sink_table", "id, name", new StagingOptions("staging", "sink_stage"), true, true),
                ReplicationMode.INCREMENTAL, 3, "updated_at", "100", Instant.now(), Instant.now(),
                250, 512, true);
    }

    private static String resolveSourcePassword(String value) {
        return "${env:SOURCE_PASSWORD}".equals(value) ? "resolved-value" : value;
    }
}
