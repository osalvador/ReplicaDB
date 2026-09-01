package org.replicadb.server.job.api;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record DatasourceResponse(
        UUID id,
        String name,
        String connectorType,
        String safeConnectDisplay,
        Map<String, String> technicalParams,
        boolean securityConfigured,
        DatasourceCapabilitiesResponse capabilities,
        boolean canView,
        boolean canUse,
        boolean canEdit,
        Instant createdAt,
        Instant updatedAt) {
}