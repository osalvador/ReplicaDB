package org.replicadb.server.security.domain;

import java.time.Instant;
import java.util.UUID;

public record DataSourcePermission(
        UUID datasourceId,
        UUID userId,
        DataSourcePermissionType permission,
        Instant createdAt) {
}
