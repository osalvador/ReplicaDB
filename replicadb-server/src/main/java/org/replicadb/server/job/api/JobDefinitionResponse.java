package org.replicadb.server.job.api;

import java.time.Instant;
import java.util.UUID;

public record JobDefinitionResponse(
        UUID id,
        String name,
        String sourceConnect,
        String sourceUser,
        String sourceTable,
        String sourceWhere,
        String sinkConnect,
        String sinkUser,
        String sinkTable,
        String mode,
        int jobs,
        String incrementalWatermarkColumn,
        String initialWatermarkValue,
        Instant createdAt,
        Instant updatedAt,
        boolean sourcePasswordConfigured,
        boolean sinkPasswordConfigured,
        String modeWarning) {
}
