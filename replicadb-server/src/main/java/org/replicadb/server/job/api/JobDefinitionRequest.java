package org.replicadb.server.job.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = false)
public record JobDefinitionRequest(
        @NotBlank(groups = Create.class)
        String name,
        @NotNull UUID sourceDatasourceId,
        Boolean sourceDatasourceUseEnabled,
        String sourceTable,
        String sourceWhere,
        String sourceColumns,
        String sourceQuery,
        @NotNull UUID sinkDatasourceId,
        Boolean sinkDatasourceUseEnabled,
        @NotBlank String sinkTable,
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
        Boolean verbose,
        @Min(1) Integer maxAttempts,
        @Min(0) Long retryBackoffSeconds,
        Boolean automaticRetryEnabled) {

    public interface Create {
    }

    @JsonAnySetter
    public void rejectUnknownProperty(String property, Object value) {
        throw new IllegalArgumentException("Unknown job request field: " + property);
    }
}
