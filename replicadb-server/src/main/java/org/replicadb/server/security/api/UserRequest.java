package org.replicadb.server.security.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.replicadb.server.security.domain.GlobalRole;

public record UserRequest(
        @NotBlank String username,
        @NotBlank String password,
        @NotNull GlobalRole role) {

    public record RoleUpdate(@NotNull GlobalRole role, boolean enabled) {
    }

    public record PasswordUpdate(@NotBlank String newPassword) {
    }
}
