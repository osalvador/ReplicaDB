package org.replicadb.server.job.api;

import org.replicadb.config.CredentialRedactor;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.domain.StagingOptions;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Component
@Profile("api")
public class JobDefinitionMapper {

    private static final String COMPLETE_MODE_WARNING =
            "Complete mode clears the sink before loading. If the run is interrupted or retried, the sink may be empty or partially populated. Use complete-atomic for an all-or-nothing load when supported.";

    public JobDefinition toDefinition(JobDefinitionRequest request, UUID id, String existingName,
                                      Instant createdAt, Instant updatedAt) {
        return toDefinition(request, id, existingName, createdAt, updatedAt,
                null, null, null, null);
    }

    public JobDefinition toDefinition(JobDefinitionRequest request, UUID id, String existingName,
                                      Instant createdAt, Instant updatedAt,
                                      RetryPolicy existingRetryPolicy, ReplicationMode existingMode,
                                      Boolean existingSourceUseEnabled, Boolean existingSinkUseEnabled) {
        String name = request.name() == null ? existingName : request.name();
        int fetchSize = request.fetchSize() == null ? 100 : request.fetchSize();
        int bandwidthThrottling = request.bandwidthThrottling() == null ? 0 : request.bandwidthThrottling();
        boolean verbose = Boolean.TRUE.equals(request.verbose());
        ReplicationMode mode = parseMode(request.mode());
        String watermarkColumn = normalizeOptional(request.incrementalWatermarkColumn());
        String initialWatermarkValue = normalizeOptional(request.initialWatermarkValue());
        if (mode == ReplicationMode.INCREMENTAL && watermarkColumn == null) {
            throw new IllegalArgumentException("incrementalWatermarkColumn is required for incremental mode");
        }
        RetryPolicy retryPolicy = retryPolicy(request, mode, existingRetryPolicy, existingMode);
        boolean sourceUseEnabled = resolveUseEnabled(request.sourceDatasourceUseEnabled(), existingSourceUseEnabled);
        boolean sinkUseEnabled = resolveUseEnabled(request.sinkDatasourceUseEnabled(), existingSinkUseEnabled);
        return new JobDefinition(
                id, name,
                new SourceEndpoint(request.sourceDatasourceId(), request.sourceTable(), request.sourceColumns(),
                        request.sourceWhere(), request.sourceQuery()),
                new SinkEndpoint(request.sinkDatasourceId(), request.sinkTable(), request.sinkColumns(),
                        stagingOptions(request.sinkStagingSchema(), request.sinkStagingTable()),
                        Boolean.TRUE.equals(request.sinkDisableEscape()),
                        Boolean.TRUE.equals(request.sinkDisableTruncate())),
                sourceUseEnabled, sinkUseEnabled, mode, request.jobs(), watermarkColumn,
                initialWatermarkValue, createdAt, updatedAt, fetchSize, bandwidthThrottling, verbose,
                retryPolicy);
    }

    private static String normalizeOptional(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
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

    public JobDefinitionResponse toResponse(JobDefinition definition) {
        return toResponse(definition, null, null);
    }

    public JobDefinitionResponse toResponse(JobDefinition definition,
                                            ManagedDataSourceSummary sourceDatasource,
                                            ManagedDataSourceSummary sinkDatasource) {
        String modeWarning = definition.mode() == ReplicationMode.COMPLETE ? COMPLETE_MODE_WARNING : null;
        return new JobDefinitionResponse(
            definition.id(), definition.name(), definition.sourceDatasourceId(), summary(sourceDatasource),
                definition.sourceDatasourceUseEnabled(), definition.sourceTable(), definition.sourceWhere(),
                definition.sourceColumns(), definition.sourceQuery(), definition.sinkDatasourceId(),
                summary(sinkDatasource), definition.sinkDatasourceUseEnabled(), definition.sinkTable(),
                definition.sinkColumns(), definition.sinkStagingSchema(), definition.sinkStagingTable(),
                definition.sinkDisableEscape(), definition.sinkDisableTruncate(), definition.mode().getModeText(),
                definition.jobs(), definition.incrementalWatermarkColumn(), definition.initialWatermarkValue(),
                definition.createdAt(), definition.updatedAt(), definition.fetchSize(), definition.bandwidthThrottling(),
                definition.verbose(),
                definition.maxAttempts(), definition.retryBackoffSeconds(), definition.automaticRetryEnabled(),
                modeWarning);
    }

    public static String completeModeWarning() {
        return COMPLETE_MODE_WARNING;
    }

    private static boolean resolveUseEnabled(Boolean requested, Boolean existing) {
        if (requested != null) {
            return requested;
        }
        return existing == null || existing;
    }

    private static DatasourceSummaryResponse summary(ManagedDataSourceSummary dataSource) {
        if (dataSource == null) {
            return null;
        }
        return new DatasourceSummaryResponse(dataSource.id(), dataSource.name(),
                dataSource.connectorType().getWireValue(),
                CredentialRedactor.redactConnectionString(dataSource.safeConnectDisplay()));
    }

    private static StagingOptions stagingOptions(String schema, String table) {
        return schema == null && table == null ? null : new StagingOptions(schema, table);
    }

    private static ReplicationMode parseMode(String modeText) {
        if (modeText == null || modeText.isBlank()) {
            throw new IllegalArgumentException("mode must not be blank");
        }
        String normalized = modeText.toLowerCase(Locale.ROOT);
        for (ReplicationMode mode : ReplicationMode.values()) {
            if (mode.getModeText().equals(normalized) || mode.name().equalsIgnoreCase(modeText)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown replication mode: " + modeText);
    }
}
