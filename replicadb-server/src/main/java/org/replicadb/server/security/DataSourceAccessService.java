package org.replicadb.server.security;

import org.replicadb.server.job.port.DataSourcePermissionStore;
import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.DataSourcePermissionType;
import org.springframework.context.annotation.Profile;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Service
@Profile("api")
public class DataSourceAccessService {

    private final DataSourcePermissionStore permissionStore;
    private final JobAccessService jobAccessService;

    public DataSourceAccessService(DataSourcePermissionStore permissionStore,
                                   JobAccessService jobAccessService) {
        this.permissionStore = permissionStore;
        this.jobAccessService = jobAccessService;
    }

    public Optional<Set<UUID>> visibleDatasourceIds(Authentication authentication) {
        if (jobAccessService.isAdmin(authentication)) {
            return Optional.empty();
        }
        UUID userId = jobAccessService.currentUserId(authentication);
        Set<UUID> ids = new LinkedHashSet<>(permissionStore.findDatasourceIdsWithPermission(
                userId, DataSourcePermissionType.VIEW));
        ids.addAll(permissionStore.findDatasourceIdsWithPermission(userId, DataSourcePermissionType.USE));
        ids.addAll(permissionStore.findDatasourceIdsWithPermission(userId, DataSourcePermissionType.EDIT));
        return Optional.of(Set.copyOf(ids));
    }

    public boolean canView(Authentication authentication, UUID datasourceId) {
        return jobAccessService.isAdmin(authentication) || hasAny(authentication, datasourceId,
                DataSourcePermissionType.VIEW, DataSourcePermissionType.USE, DataSourcePermissionType.EDIT);
    }

    public boolean canUse(Authentication authentication, UUID datasourceId) {
        return jobAccessService.isAdmin(authentication) || has(authentication, datasourceId,
                DataSourcePermissionType.USE);
    }

    public boolean canEdit(Authentication authentication, UUID datasourceId) {
        return jobAccessService.isAdmin(authentication) || has(authentication, datasourceId,
                DataSourcePermissionType.EDIT);
    }

    public void requireView(Authentication authentication, UUID datasourceId) {
        if (!canView(authentication, datasourceId)) {
            throw new AccessDeniedException("Access denied");
        }
    }

    public void requireEdit(Authentication authentication, UUID datasourceId) {
        if (!canEdit(authentication, datasourceId)) {
            throw new AccessDeniedException("Access denied");
        }
    }

    public void requireUse(Authentication authentication, UUID datasourceId) {
        if (!canUse(authentication, datasourceId)) {
            throw new AccessDeniedException("Access denied");
        }
    }

    private boolean hasAny(Authentication authentication, UUID datasourceId,
                           DataSourcePermissionType... permissions) {
        for (DataSourcePermissionType permission : permissions) {
            if (has(authentication, datasourceId, permission)) {
                return true;
            }
        }
        return false;
    }

    private boolean has(Authentication authentication, UUID datasourceId,
                        DataSourcePermissionType permission) {
        UUID userId = jobAccessService.currentUserId(authentication);
        return permissionStore.hasPermission(datasourceId, userId, permission);
    }
}
