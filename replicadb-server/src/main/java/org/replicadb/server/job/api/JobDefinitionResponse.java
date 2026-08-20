package org.replicadb.server.job.api;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobDefinitionResponse(
        UUID id,
        String name,
        String sourceConnect,
        String sourceUser,
        String sourceTable,
        String sourceWhere,
        String sourceAuthMode,
        String sourceAuthPrincipalId,
        String sourceAuthLoginHint,
        String sourceAuthClientCertificate,
        String sourceAuthClientKey,
        Map<String, String> sourceConnectionParams,
        String sourceColumns,
        String sourceQuery,
        String sinkConnect,
        String sinkUser,
        String sinkTable,
        String sinkAuthMode,
        String sinkAuthPrincipalId,
        String sinkAuthLoginHint,
        String sinkAuthClientCertificate,
        String sinkAuthClientKey,
        Map<String, String> sinkConnectionParams,
        String sinkColumns,
        String sinkStagingSchema,
        String sinkStagingTable,
        boolean sinkDisableEscape,
        boolean sinkDisableTruncate,
        String mode,
        int jobs,
        String incrementalWatermarkColumn,
        String initialWatermarkValue,
        Instant createdAt,
        Instant updatedAt,
        int fetchSize,
        int bandwidthThrottling,
        boolean verbose,
        boolean sourcePasswordConfigured,
        boolean sinkPasswordConfigured,
        int maxAttempts,
        long retryBackoffSeconds,
        boolean automaticRetryEnabled,
        String modeWarning) {

    public JobDefinitionResponse(UUID id, String name, String sourceConnect, String sourceUser,
                                 String sourceTable, String sourceWhere, String sinkConnect, String sinkUser,
                                 String sinkTable, String mode, int jobs, String incrementalWatermarkColumn,
                                 String initialWatermarkValue, Instant createdAt, Instant updatedAt,
                                 boolean sourcePasswordConfigured, boolean sinkPasswordConfigured,
                                 String modeWarning) {
        this(id, name, sourceConnect, sourceUser, sourceTable, sourceWhere,
                null, null, null, null, null, Map.of(), null, null,
                sinkConnect, sinkUser, sinkTable,
                null, null, null, null, null, Map.of(), null, null, null, false, false,
                mode, jobs, incrementalWatermarkColumn, initialWatermarkValue, createdAt, updatedAt,
                100, 0, false, sourcePasswordConfigured, sinkPasswordConfigured,
                3, 60, false, modeWarning);
    }
}
