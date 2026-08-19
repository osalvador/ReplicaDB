package org.replicadb.server.job.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record JobDefinitionRequest(
        @NotBlank(groups = Create.class)
        String name,
        @NotBlank String sourceConnect,
        String sourceUser,
        String sourcePassword,
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
        @NotBlank String sinkConnect,
        String sinkUser,
        String sinkPassword,
        @NotBlank String sinkTable,
        String sinkAuthMode,
        String sinkAuthPrincipalId,
        String sinkAuthLoginHint,
        String sinkAuthClientCertificate,
        String sinkAuthClientKey,
        Map<String, String> sinkConnectionParams,
        String sinkColumns,
        String sinkStagingSchema,
        String sinkStagingTable,
        Boolean sinkDisableEscape,
        Boolean sinkDisableTruncate,
        @NotBlank String mode,
        @Min(1) int jobs,
        String incrementalWatermarkColumn,
        String initialWatermarkValue,
        @Min(1) Integer fetchSize,
        @Min(0) Integer bandwidthThrottling,
        Boolean verbose) {

    public JobDefinitionRequest(String name, String sourceConnect, String sourceUser, String sourcePassword,
                                 String sourceTable, String sourceWhere, String sinkConnect, String sinkUser,
                                 String sinkPassword, String sinkTable, String mode, int jobs,
                                 String incrementalWatermarkColumn, String initialWatermarkValue) {
        this(name, sourceConnect, sourceUser, sourcePassword, sourceTable, sourceWhere,
                null, null, null, null, null, Map.of(), null, null,
                sinkConnect, sinkUser, sinkPassword, sinkTable,
                null, null, null, null, null, Map.of(), null, null, null, null, null,
                mode, jobs, incrementalWatermarkColumn, initialWatermarkValue,
                null, null, null);
    }

    public interface Create {
    }
}
