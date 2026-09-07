package org.replicadb.server.job.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = false)
@Schema(description = "Create or update contract for one managed, single-table replication job.")
public record JobDefinitionRequest(
    @Schema(description = "Immutable display name. Required on create and omitted or unchanged on update.", example = "Nightly orders", nullable = true)
        @NotBlank(groups = Create.class)
        String name,
    @Schema(description = "Source datasource identifier.", format = "uuid") @NotNull UUID sourceDatasourceId,
    @Schema(description = "Whether future attempts may resolve and use the source binding.", nullable = true) Boolean sourceDatasourceUseEnabled,
    @Schema(description = "Source table or collection. Mutually exclusive with sourceQuery.", nullable = true) String sourceTable,
    @Schema(description = "Connector-specific source predicate without a WHERE keyword.", nullable = true) String sourceWhere,
    @Schema(description = "Ordered source column selection or connector projection.", nullable = true) String sourceColumns,
    @Schema(description = "Connector-specific free-form source query. Mutually exclusive with sourceTable.", nullable = true) String sourceQuery,
    @Schema(description = "Sink datasource identifier.", format = "uuid") @NotNull UUID sinkDatasourceId,
    @Schema(description = "Whether future attempts may resolve and use the sink binding.", nullable = true) Boolean sinkDatasourceUseEnabled,
    @Schema(description = "Sink table, collection, topic, or object target.") @NotBlank String sinkTable,
    @Schema(description = "Ordered sink column mapping.", nullable = true) String sinkColumns,
    @Schema(description = "Schema in which ReplicaDB may create a staging table.", nullable = true) String sinkStagingSchema,
    @Schema(description = "Existing staging table managed by the operator.", nullable = true) String sinkStagingTable,
    @Schema(description = "Connector-specific escaping override.", nullable = true) Boolean sinkDisableEscape,
    @Schema(description = "Skip complete-mode sink truncation when supported.", nullable = true) Boolean sinkDisableTruncate,
    @Schema(description = "Replication mode.", allowableValues = {"complete", "complete-atomic", "incremental"}, example = "complete")
    @NotBlank String mode,
    @Schema(description = "Parallel tasks inside this run.", minimum = "1", example = "1") @Min(1) int jobs,
    @Schema(description = "Source column used to compute the next incremental watermark.", nullable = true) String incrementalWatermarkColumn,
    @Schema(description = "Initial committed watermark for the first managed run.", nullable = true) String initialWatermarkValue,
    @Schema(description = "Rows requested per source fetch.", minimum = "1", nullable = true) @Min(1) Integer fetchSize,
    @Schema(description = "Per-task bandwidth cap in KB/s; zero means unlimited.", minimum = "0", nullable = true) @Min(0) Integer bandwidthThrottling,
    @Schema(description = "Enable verbose replication diagnostics.", nullable = true) Boolean verbose,
    @Schema(description = "Maximum attempts including the initial attempt.", minimum = "1", nullable = true) @Min(1) Integer maxAttempts,
    @Schema(description = "Delay before an automatic retry becomes eligible, in seconds.", minimum = "0", nullable = true) @Min(0) Long retryBackoffSeconds,
    @Schema(description = "Whether lease-expiry recovery may create another attempt.", nullable = true) Boolean automaticRetryEnabled) {

    public interface Create {
    }

    @JsonAnySetter
    public void rejectUnknownProperty(String property, Object value) {
        throw new IllegalArgumentException("Unknown job request field: " + property);
    }
}
