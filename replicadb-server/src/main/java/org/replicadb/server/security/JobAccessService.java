package org.replicadb.server.security;

import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Service
public class JobAccessService {

    private final JobPermissionRepository permissionRepository;

    public JobAccessService(JobPermissionRepository permissionRepository) {
        this.permissionRepository = permissionRepository;
    }

    public void require(Authentication authentication, UUID jobDefinitionId, JobPermissionType permission) {
        if (isAdmin(authentication)) {
            return;
        }
        UUID userId = currentUserId(authentication);
        if (!permissionRepository.hasPermission(jobDefinitionId, userId, permission)) {
            throw new AccessDeniedException("Access denied");
        }
    }

    public Optional<Set<UUID>> visibleJobIds(Authentication authentication) {
        if (isAdmin(authentication)) {
            return Optional.empty();
        }
        return Optional.of(permissionRepository.findJobIdsWithPermission(
                currentUserId(authentication), JobPermissionType.VIEW));
    }

    public UUID currentUserId(Authentication authentication) {
        if (authentication != null && authentication.getPrincipal() instanceof ReplicaDbUserDetails details
                && details.userId() != null) {
            return details.userId();
        }
        throw new IllegalStateException("Unsupported authenticated principal");
    }

    public boolean isAdmin(Authentication authentication) {
        return authentication != null && authentication.getAuthorities().stream()
                .anyMatch(authority -> "ROLE_ADMIN".equals(authority.getAuthority()));
    }
}
