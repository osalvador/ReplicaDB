package org.replicadb.server.security.api;

import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.GlobalRole;
import org.springframework.security.core.Authentication;

import java.util.UUID;

public record UserIdentityResponse(UUID id, String username, GlobalRole role) {

    public static UserIdentityResponse from(Authentication authentication) {
        if (!(authentication.getPrincipal() instanceof ReplicaDbUserDetails details)) {
            throw new IllegalStateException("Unsupported authenticated principal");
        }
        return new UserIdentityResponse(details.userId(), details.getUsername(), details.appUser().role());
    }
}
