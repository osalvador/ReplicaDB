package org.replicadb.server.job.api;

import java.time.Instant;
import java.util.UUID;

public record JobDefinitionResponse(
        UUID id,
        String name,
    UUID sourceDatasourceId,
    DatasourceSummaryResponse sourceDatasource,
    boolean sourceDatasourceUseEnabled,
        String sourceTable,
        String sourceWhere,
        String sourceColumns,
        String sourceQuery,
    UUID sinkDatasourceId,
    DatasourceSummaryResponse sinkDatasource,
    boolean sinkDatasourceUseEnabled,
        String sinkTable,
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
        int maxAttempts,
        long retryBackoffSeconds,
        boolean automaticRetryEnabled,
        String modeWarning) {
}
