package org.replicadb.server.security;

import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.AppUser;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.test.context.support.WithSecurityContextFactory;

import java.util.UUID;

public class WithMockReplicaDbUserSecurityContextFactory
        implements WithSecurityContextFactory<WithMockReplicaDbUser> {

    @Override
    public SecurityContext createSecurityContext(WithMockReplicaDbUser annotation) {
        AppUser user = new AppUser(UUID.fromString(annotation.userId()), annotation.username(),
                "test-password-hash", annotation.role(), true, null, null);
        ReplicaDbUserDetails details = new ReplicaDbUserDetails(user);
        UsernamePasswordAuthenticationToken authentication =
                new UsernamePasswordAuthenticationToken(details, null, details.getAuthorities());
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        return context;
    }
}
