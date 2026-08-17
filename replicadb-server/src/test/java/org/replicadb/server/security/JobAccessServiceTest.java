package org.replicadb.server.security;

import org.junit.jupiter.api.Test;
import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JobAccessServiceTest {

    private final JobPermissionRepository repository = mock(JobPermissionRepository.class);
    private final JobAccessService service = new JobAccessService(repository);

    @Test
    void adminBypassesEveryPermission() {
        Authentication admin = authentication("admin", "ROLE_ADMIN");

        service.require(admin, UUID.randomUUID(), JobPermissionType.CANCEL);

        verifyNoInteractions(repository);
    }

    @Test
    void matchingPermissionAllowsNonAdmin() {
        UUID jobId = UUID.randomUUID();
        Authentication operator = userAuthentication(GlobalRole.OPERATOR);
        when(repository.hasPermission(jobId, service.currentUserId(operator), JobPermissionType.VIEW))
                .thenReturn(true);

        service.require(operator, jobId, JobPermissionType.VIEW);

        verify(repository).hasPermission(jobId, service.currentUserId(operator), JobPermissionType.VIEW);
    }

    @Test
    void missingPermissionIsDenied() {
        UUID jobId = UUID.randomUUID();
        Authentication viewer = userAuthentication(GlobalRole.VIEWER);
        when(repository.hasPermission(jobId, service.currentUserId(viewer), JobPermissionType.EDIT))
                .thenReturn(false);

        assertThrows(AccessDeniedException.class,
                () -> service.require(viewer, jobId, JobPermissionType.EDIT));
    }

    @Test
    void visibleJobsDistinguishAdminFromEmptyAcl() {
        Authentication admin = authentication("admin", "ROLE_ADMIN");
        Authentication viewer = userAuthentication(GlobalRole.VIEWER);
        when(repository.findJobIdsWithPermission(service.currentUserId(viewer), JobPermissionType.VIEW))
                .thenReturn(Set.of());

        assertEquals(Optional.empty(), service.visibleJobIds(admin));
        assertEquals(Optional.of(Set.of()), service.visibleJobIds(viewer));
    }

    @Test
    void rejectsUnsupportedNonAdminPrincipal() {
        Authentication authentication = authentication("operator", "ROLE_OPERATOR");

        assertThrows(IllegalStateException.class,
                () -> service.currentUserId(authentication));
        assertThrows(IllegalStateException.class,
                () -> service.require(authentication, UUID.randomUUID(), JobPermissionType.VIEW));
    }

    private static Authentication userAuthentication(GlobalRole role) {
        AppUser user = new AppUser(UUID.randomUUID(), "acl-user", "hash", role, true, null, null);
        ReplicaDbUserDetails details = new ReplicaDbUserDetails(user);
        return new UsernamePasswordAuthenticationToken(details, null, details.getAuthorities());
    }

    private static Authentication authentication(String principal, String role) {
        return new UsernamePasswordAuthenticationToken(principal, null,
                List.of(new SimpleGrantedAuthority(role)));
    }
}
