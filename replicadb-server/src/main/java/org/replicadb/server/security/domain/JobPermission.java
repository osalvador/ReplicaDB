package org.replicadb.server.security.domain;

import java.time.Instant;
import java.util.UUID;

public record JobPermission(
        UUID jobDefinitionId,
        UUID userId,
        JobPermissionType permission,
        Instant createdAt) {

    public JobPermission {
        if (jobDefinitionId == null) {
            throw new IllegalArgumentException("jobDefinitionId must not be null");
        }
        if (userId == null) {
            throw new IllegalArgumentException("userId must not be null");
        }
        if (permission == null) {
            throw new IllegalArgumentException("permission must not be null");
        }
    }
}
