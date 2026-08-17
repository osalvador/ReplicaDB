package org.replicadb.server.security.api;

import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;

import java.time.Instant;
import java.util.UUID;

public record UserResponse(
        UUID id,
        String username,
        GlobalRole role,
        boolean enabled,
        Instant createdAt,
        Instant updatedAt) {

    public static UserResponse from(AppUser user) {
        return new UserResponse(user.id(), user.username(), user.role(), user.enabled(),
                user.createdAt(), user.updatedAt());
    }
}
