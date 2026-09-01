package org.replicadb.server.security;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.replicadb.server.job.port.DataSourcePermissionStore;
import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.DataSourcePermissionType;
import org.replicadb.server.security.domain.GlobalRole;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataSourceAccessServiceTest {

    private final DataSourcePermissionStore permissionStore = Mockito.mock(DataSourcePermissionStore.class);
    private final JobAccessService jobAccessService = Mockito.mock(JobAccessService.class);
    private final DataSourceAccessService service = new DataSourceAccessService(permissionStore, jobAccessService);
    private final UUID userId = UUID.randomUUID();
    private final UUID datasourceId = UUID.randomUUID();

    @Test
    void editImpliesSafeViewAndListVisibility() {
        Authentication authentication = authentication(GlobalRole.OPERATOR);
        when(jobAccessService.isAdmin(authentication)).thenReturn(false);
        when(jobAccessService.currentUserId(authentication)).thenReturn(userId);
        when(permissionStore.findDatasourceIdsWithPermission(userId, DataSourcePermissionType.VIEW))
                .thenReturn(Set.of());
        when(permissionStore.findDatasourceIdsWithPermission(userId, DataSourcePermissionType.USE))
                .thenReturn(Set.of());
        when(permissionStore.findDatasourceIdsWithPermission(userId, DataSourcePermissionType.EDIT))
                .thenReturn(Set.of(datasourceId));
        when(permissionStore.hasPermission(datasourceId, userId, DataSourcePermissionType.EDIT))
                .thenReturn(true);

        assertTrue(service.canView(authentication, datasourceId));
        assertEquals(Set.of(datasourceId), service.visibleDatasourceIds(authentication).orElseThrow());
        verify(permissionStore).findDatasourceIdsWithPermission(userId, DataSourcePermissionType.EDIT);
    }

    @Test
    void viewDoesNotImplyUseOrEdit() {
        Authentication authentication = authentication(GlobalRole.VIEWER);
        when(jobAccessService.isAdmin(authentication)).thenReturn(false);
        when(jobAccessService.currentUserId(authentication)).thenReturn(userId);
        when(permissionStore.hasPermission(datasourceId, userId, DataSourcePermissionType.VIEW))
                .thenReturn(true);

        assertTrue(service.canView(authentication, datasourceId));
        assertEquals(false, service.canUse(authentication, datasourceId));
        assertEquals(false, service.canEdit(authentication, datasourceId));
        assertThrows(AccessDeniedException.class, () -> service.requireUse(authentication, datasourceId));
    }

    @Test
    void adminBypassesDatasourceAcl() {
        Authentication authentication = authentication(GlobalRole.ADMIN);
        when(jobAccessService.isAdmin(authentication)).thenReturn(true);

        assertTrue(service.canView(authentication, datasourceId));
        assertTrue(service.canUse(authentication, datasourceId));
        assertTrue(service.canEdit(authentication, datasourceId));
        verify(permissionStore, Mockito.never()).hasPermission(Mockito.any(), Mockito.any(), Mockito.any());
    }

    private Authentication authentication(GlobalRole role) {
        AppUser user = new AppUser(userId, "access-user", "hash", role, true, null, null);
        return new UsernamePasswordAuthenticationToken(new ReplicaDbUserDetails(user), null,
                new ReplicaDbUserDetails(user).getAuthorities());
    }
}
