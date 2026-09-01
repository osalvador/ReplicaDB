package org.replicadb.server.job.api;

import org.replicadb.server.security.domain.DataSourcePermissionType;

import java.util.Set;
import java.util.UUID;

public record DatasourcePermissionResponse(
        UUID userId,
        String username,
        Set<DataSourcePermissionType> permissions) {
}
