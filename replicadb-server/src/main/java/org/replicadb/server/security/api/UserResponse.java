package org.replicadb.server.security.api;

import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Public user administration record. Password hashes and credentials are never exposed.")
public record UserResponse(
    @Schema(description = "User identifier.", format = "uuid") UUID id,
    @Schema(description = "Current username.") String username,
    @Schema(description = "Global authorization role.") GlobalRole role,
    @Schema(description = "Whether the account may authenticate.") boolean enabled,
    @Schema(description = "Creation timestamp in UTC.", format = "date-time") Instant createdAt,
    @Schema(description = "Last update timestamp in UTC.", format = "date-time") Instant updatedAt) {

    public static UserResponse from(AppUser user) {
        return new UserResponse(user.id(), user.username(), user.role(), user.enabled(),
                user.createdAt(), user.updatedAt());
    }
}
