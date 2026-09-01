package org.replicadb.server.job.api;

import java.util.UUID;

public record DatasourceSummaryResponse(
        UUID id,
        String name,
        String connectorType,
        String safeConnectDisplay) {
}
