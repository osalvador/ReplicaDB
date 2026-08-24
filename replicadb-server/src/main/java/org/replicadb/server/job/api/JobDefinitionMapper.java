package org.replicadb.server.job.api;

import org.replicadb.config.CredentialRedactor;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ConnectionCredentials;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.domain.StagingOptions;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Component
@Profile("api")
public class JobDefinitionMapper {

        private static final String COMPLETE_MODE_WARNING =
            "Complete mode clears the sink before loading. If the run is interrupted or retried, the sink may be empty or partially populated. Use complete-atomic for an all-or-nothing load when supported.";

    public JobDefinition toDefinition(JobDefinitionRequest request, UUID id, String existingName,
                                      Instant createdAt, Instant updatedAt) {
        return buildDefinition(request, id, existingName, createdAt, updatedAt,
            request.sourcePassword(), request.sinkPassword(), null, null);
        }

        public JobDefinition toDefinition(JobDefinitionRequest request, UUID id, String existingName,
                          Instant createdAt, Instant updatedAt,
                          String existingSourcePassword, String existingSinkPassword) {
        return toDefinition(request, id, existingName, createdAt, updatedAt,
            existingSourcePassword, existingSinkPassword, null, null);
        }

        public JobDefinition toDefinition(JobDefinitionRequest request, UUID id, String existingName,
                          Instant createdAt, Instant updatedAt,
                          String existingSourcePassword, String existingSinkPassword,
                          RetryPolicy existingRetryPolicy, ReplicationMode existingMode) {
        String sourcePassword = resolvePassword(request.sourcePassword(), existingSourcePassword);
        String sinkPassword = resolvePassword(request.sinkPassword(), existingSinkPassword);
        return buildDefinition(request, id, existingName, createdAt, updatedAt,
            sourcePassword, sinkPassword, existingRetryPolicy, existingMode);
        }

        private JobDefinition buildDefinition(JobDefinitionRequest request, UUID id, String existingName,
                           Instant createdAt, Instant updatedAt,
                           String sourcePassword, String sinkPassword,
                           RetryPolicy existingRetryPolicy, ReplicationMode existingMode) {
        String name = request.name() == null ? existingName : request.name();
        int fetchSize = request.fetchSize() == null ? 100 : request.fetchSize();
        int bandwidthThrottling = request.bandwidthThrottling() == null ? 0 : request.bandwidthThrottling();
        boolean verbose = Boolean.TRUE.equals(request.verbose());
        ReplicationMode mode = parseMode(request.mode());
        RetryPolicy retryPolicy = retryPolicy(request, mode, existingRetryPolicy, existingMode);
        return new JobDefinition(
            id, name,
            new SourceEndpoint(
                new ConnectionCredentials(
                    request.sourceConnect(), request.sourceUser(), sourcePassword,
                    new AzureAuthentication(request.sourceAuthMode(), request.sourceAuthPrincipalId(),
                        request.sourceAuthLoginHint(), request.sourceAuthClientCertificate(),
                        request.sourceAuthClientKey()), request.sourceConnectionParams()),
                request.sourceTable(), request.sourceColumns(), request.sourceWhere(), request.sourceQuery()),
            new SinkEndpoint(
                new ConnectionCredentials(
                    request.sinkConnect(), request.sinkUser(), sinkPassword,
                    new AzureAuthentication(request.sinkAuthMode(), request.sinkAuthPrincipalId(),
                        request.sinkAuthLoginHint(), request.sinkAuthClientCertificate(),
                        request.sinkAuthClientKey()), request.sinkConnectionParams()),
                request.sinkTable(), request.sinkColumns(), stagingOptions(request.sinkStagingSchema(),
                    request.sinkStagingTable()), Boolean.TRUE.equals(request.sinkDisableEscape()),
                Boolean.TRUE.equals(request.sinkDisableTruncate())),
            mode, request.jobs(), request.incrementalWatermarkColumn(), request.initialWatermarkValue(),
            createdAt, updatedAt, fetchSize, bandwidthThrottling, verbose, retryPolicy);
    }

        private static RetryPolicy retryPolicy(JobDefinitionRequest request, ReplicationMode mode,
                           RetryPolicy existingRetryPolicy, ReplicationMode existingMode) {
        boolean noPolicyFields = request.maxAttempts() == null
            && request.retryBackoffSeconds() == null
            && request.automaticRetryEnabled() == null;
        if (noPolicyFields && existingRetryPolicy != null && mode == existingMode) {
            return existingRetryPolicy;
        }
        RetryPolicy base = existingRetryPolicy != null && mode == existingMode
            ? existingRetryPolicy : RetryPolicy.defaultsFor(mode);
        return new RetryPolicy(
            request.maxAttempts() == null ? base.maxAttempts() : request.maxAttempts(),
            request.retryBackoffSeconds() == null
                ? base.retryBackoffSeconds() : request.retryBackoffSeconds(),
            request.automaticRetryEnabled() == null
                ? base.automaticRetryEnabled() : request.automaticRetryEnabled());
        }

        private static String resolvePassword(String requestedPassword, String existingPassword) {
        return requestedPassword == null || requestedPassword.isBlank() ? existingPassword : requestedPassword;
    }

    public JobDefinitionResponse toResponse(JobDefinition definition) {
        String modeWarning = definition.mode() == ReplicationMode.COMPLETE ? COMPLETE_MODE_WARNING : null;
        return new JobDefinitionResponse(
            definition.id(), definition.name(), CredentialRedactor.redactConnectionString(definition.sourceConnect()),
            definition.sourceUser(),
                definition.sourceTable(), definition.sourceWhere(), definition.sourceAuthentication().mode(),
                definition.sourceAuthentication().principalId(), definition.sourceAuthentication().loginHint(),
                definition.sourceAuthentication().clientCertificate(), definition.sourceAuthentication().clientKey(),
                redactConnectionParams(definition.sourceConnectionParams()), definition.sourceColumns(),
                definition.sourceQuery(),
                CredentialRedactor.redactConnectionString(definition.sinkConnect()), definition.sinkUser(),
                definition.sinkTable(), definition.sinkAuthentication().mode(), definition.sinkAuthentication().principalId(),
                definition.sinkAuthentication().loginHint(), definition.sinkAuthentication().clientCertificate(),
                definition.sinkAuthentication().clientKey(), redactConnectionParams(definition.sinkConnectionParams()),
                definition.sinkColumns(), definition.sinkStagingSchema(), definition.sinkStagingTable(),
                definition.sinkDisableEscape(), definition.sinkDisableTruncate(), definition.mode().getModeText(),
                definition.jobs(), definition.incrementalWatermarkColumn(), definition.initialWatermarkValue(),
                definition.createdAt(), definition.updatedAt(), definition.fetchSize(), definition.bandwidthThrottling(),
                definition.verbose(),
                definition.sourcePassword() != null, definition.sinkPassword() != null,
                definition.maxAttempts(), definition.retryBackoffSeconds(), definition.automaticRetryEnabled(),
                modeWarning);
    }

    public static String completeModeWarning() {
        return COMPLETE_MODE_WARNING;
    }

    private static StagingOptions stagingOptions(String schema, String table) {
        return schema == null && table == null ? null : new StagingOptions(schema, table);
    }

    private static Map<String, String> redactConnectionParams(Map<String, String> params) {
        Properties properties = new Properties();
        if (params != null) {
            params.forEach(properties::setProperty);
        }
        Properties redacted = CredentialRedactor.redactProperties(properties);
        Map<String, String> result = new HashMap<>();
        redacted.stringPropertyNames().forEach(key -> result.put(key, redacted.getProperty(key)));
        return Map.copyOf(result);
    }

    private static ReplicationMode parseMode(String modeText) {
        String normalized = modeText.toLowerCase(Locale.ROOT);
        for (ReplicationMode mode : ReplicationMode.values()) {
            if (mode.getModeText().equals(normalized) || mode.name().equalsIgnoreCase(modeText)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown replication mode: " + modeText);
    }
}
