package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Persisted managed job definition with resolved policy values and redacted datasource summaries.")
public record JobDefinitionResponse(
        @Schema(description = "Job definition identifier.", format = "uuid") UUID id,
        @Schema(description = "Immutable job display name.") String name,
        @Schema(description = "Source datasource identifier.", format = "uuid") UUID sourceDatasourceId,
        @Schema(description = "Redacted source summary, omitted when the caller cannot view it.", nullable = true) DatasourceSummaryResponse sourceDatasource,
        @Schema(description = "Whether future attempts may use the source binding.") boolean sourceDatasourceUseEnabled,
        @Schema(description = "Source table or collection.", nullable = true) String sourceTable,
        @Schema(description = "Connector-specific source predicate.", nullable = true) String sourceWhere,
        @Schema(description = "Ordered source column selection.", nullable = true) String sourceColumns,
        @Schema(description = "Connector-specific free-form source query.", nullable = true) String sourceQuery,
        @Schema(description = "Sink datasource identifier.", format = "uuid") UUID sinkDatasourceId,
        @Schema(description = "Redacted sink summary, omitted when the caller cannot view it.", nullable = true) DatasourceSummaryResponse sinkDatasource,
        @Schema(description = "Whether future attempts may use the sink binding.") boolean sinkDatasourceUseEnabled,
        @Schema(description = "Sink target name.") String sinkTable,
        @Schema(description = "Ordered sink column mapping.", nullable = true) String sinkColumns,
        @Schema(description = "Schema for generated staging tables.", nullable = true) String sinkStagingSchema,
        @Schema(description = "Operator-managed staging table.", nullable = true) String sinkStagingTable,
        @Schema(description = "Whether connector escaping is disabled.") boolean sinkDisableEscape,
        @Schema(description = "Whether complete-mode truncation is disabled.") boolean sinkDisableTruncate,
        @Schema(description = "Replication mode.", allowableValues = {"complete", "complete-atomic", "incremental"}) String mode,
        @Schema(description = "Parallel tasks inside each run.", minimum = "1") int jobs,
        @Schema(description = "Incremental watermark source column.", nullable = true) String incrementalWatermarkColumn,
        @Schema(description = "Initial committed watermark.", nullable = true) String initialWatermarkValue,
        @Schema(description = "Creation timestamp in UTC.", format = "date-time") Instant createdAt,
        @Schema(description = "Last update timestamp in UTC.", format = "date-time") Instant updatedAt,
        @Schema(description = "Rows requested per source fetch.", minimum = "1") int fetchSize,
        @Schema(description = "Per-task bandwidth cap in KB/s; zero means unlimited.", minimum = "0") int bandwidthThrottling,
        @Schema(description = "Whether verbose replication diagnostics are enabled.") boolean verbose,
        @Schema(description = "Maximum attempts including the initial attempt.", minimum = "1") int maxAttempts,
        @Schema(description = "Retry eligibility delay in seconds.", minimum = "0") long retryBackoffSeconds,
        @Schema(description = "Whether lease-expiry recovery may create another attempt.") boolean automaticRetryEnabled,
        @Schema(description = "Mode-specific warning for destructive or indeterminate outcomes.", nullable = true) String modeWarning) {
}
