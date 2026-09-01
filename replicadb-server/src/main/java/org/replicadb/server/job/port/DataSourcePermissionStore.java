package org.replicadb.server.job.port;

import org.replicadb.server.security.domain.DataSourcePermission;
import org.replicadb.server.security.domain.DataSourcePermissionType;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface DataSourcePermissionStore {

    void grant(UUID datasourceId, UUID userId, DataSourcePermissionType permission);

    void replace(UUID datasourceId, UUID userId, Set<DataSourcePermissionType> permissions);

    void revoke(UUID datasourceId, UUID userId, DataSourcePermissionType permission);

    void revokeAll(UUID datasourceId, UUID userId);

    boolean hasPermission(UUID datasourceId, UUID userId, DataSourcePermissionType permission);

    Set<UUID> findDatasourceIdsWithPermission(UUID userId, DataSourcePermissionType permission);

    List<DataSourcePermission> findByDatasourceId(UUID datasourceId);
}
