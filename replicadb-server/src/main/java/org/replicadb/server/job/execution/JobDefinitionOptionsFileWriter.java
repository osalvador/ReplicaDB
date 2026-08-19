package org.replicadb.server.job.execution;

import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.JobDefinition;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

@Component
public class JobDefinitionOptionsFileWriter {

    private static final Set<PosixFilePermission> OWNER_ONLY_PERMISSIONS =
            PosixFilePermissions.fromString("rw-------");

    public Path write(JobDefinition definition, String previousWatermarkValue,
                      Function<String, String> valueResolver) throws IOException {
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(valueResolver, "valueResolver must not be null");

        Path path = Files.createTempFile("replicadb-job-", ".conf");
        try {
            setOwnerOnlyPermissions(path);
            Properties properties = new Properties();
            add(properties, "mode", definition.mode().getModeText(), valueResolver);
            add(properties, "jobs", Integer.toString(definition.jobs()), valueResolver);
            add(properties, "fetch.size", Integer.toString(definition.fetchSize()), valueResolver);
            add(properties, "bandwidth.throttling", Integer.toString(definition.bandwidthThrottling()), valueResolver);
            add(properties, "verbose", Boolean.toString(definition.verbose()), valueResolver);

            add(properties, "source.connect", definition.sourceConnect(), valueResolver);
            add(properties, "source.user", definition.sourceUser(), valueResolver);
            add(properties, "source.password", definition.sourcePassword(), valueResolver);
            addAuthentication(properties, "source", definition.sourceAuthentication(), valueResolver);
            add(properties, "source.table", definition.sourceTable(), valueResolver);
            add(properties, "source.columns", definition.sourceColumns(), valueResolver);
            add(properties, "source.where", definition.sourceWhere(), valueResolver);
            add(properties, "source.query", definition.sourceQuery(), valueResolver);
            addConnectionParams(properties, "source", definition.sourceConnectionParams(), valueResolver);

            add(properties, "sink.connect", definition.sinkConnect(), valueResolver);
            add(properties, "sink.user", definition.sinkUser(), valueResolver);
            add(properties, "sink.password", definition.sinkPassword(), valueResolver);
            addAuthentication(properties, "sink", definition.sinkAuthentication(), valueResolver);
            add(properties, "sink.table", definition.sinkTable(), valueResolver);
            add(properties, "sink.columns", definition.sinkColumns(), valueResolver);
            add(properties, "sink.staging.schema", definition.sinkStagingSchema(), valueResolver);
            add(properties, "sink.staging.table", definition.sinkStagingTable(), valueResolver);
            add(properties, "sink.disable.escape", Boolean.toString(definition.sinkDisableEscape()), valueResolver);
            add(properties, "sink.disable.truncate", Boolean.toString(definition.sinkDisableTruncate()), valueResolver);
            addConnectionParams(properties, "sink", definition.sinkConnectionParams(), valueResolver);

            add(properties, "incremental.watermark.column", definition.incrementalWatermarkColumn(), valueResolver);
            if (definition.incrementalWatermarkColumn() != null) {
                String watermarkValue = previousWatermarkValue == null
                        ? definition.initialWatermarkValue()
                        : previousWatermarkValue;
                add(properties, "incremental.watermark.value", watermarkValue, valueResolver);
            }

            try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                properties.store(writer, null);
            }
            return path;
        } catch (IOException | RuntimeException exception) {
            Files.deleteIfExists(path);
            throw exception;
        }
    }

    private static void addAuthentication(Properties properties, String prefix,
                                           AzureAuthentication authentication,
                                           Function<String, String> valueResolver) {
        add(properties, prefix + ".auth.mode", authentication.mode(), valueResolver);
        add(properties, prefix + ".auth.principal.id", authentication.principalId(), valueResolver);
        add(properties, prefix + ".auth.login.hint", authentication.loginHint(), valueResolver);
        add(properties, prefix + ".auth.client.certificate", authentication.clientCertificate(), valueResolver);
        add(properties, prefix + ".auth.client.key", authentication.clientKey(), valueResolver);
    }

    private static void addConnectionParams(Properties properties, String prefix,
                                            Map<String, String> connectionParams,
                                            Function<String, String> valueResolver) {
        for (Map.Entry<String, String> entry : connectionParams.entrySet()) {
            add(properties, prefix + ".connect.parameter." + entry.getKey(), entry.getValue(), valueResolver);
        }
    }

    private static void add(Properties properties, String key, String value,
                            Function<String, String> valueResolver) {
        if (value == null || value.isBlank()) {
            return;
        }
        String resolved = valueResolver.apply(value);
        if (resolved != null) {
            properties.setProperty(key, resolved);
        }
    }

    private static void setOwnerOnlyPermissions(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, OWNER_ONLY_PERMISSIONS);
        } catch (UnsupportedOperationException ignored) {
            // The default permissions are used on non-POSIX filesystems.
        }
    }
}
