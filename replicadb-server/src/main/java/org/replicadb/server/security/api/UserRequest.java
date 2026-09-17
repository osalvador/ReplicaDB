package org.replicadb.server.security.api;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.replicadb.server.security.domain.GlobalRole;

@Schema(description = "ADMIN request for creating a user.")
public record UserRequest(
    @Schema(description = "Unique username.", example = "replication-operator") @NotBlank String username,
    @Schema(description = "Initial password accepted only for hashing and never returned.", accessMode = Schema.AccessMode.WRITE_ONLY)
    @NotBlank String password,
    @Schema(description = "Initial global role.") @NotNull GlobalRole role) {

    @Schema(description = "ADMIN update for a user's global role and enabled state.")
    public record RoleUpdate(
        @Schema(description = "Replacement global role.") @NotNull GlobalRole role,
        @Schema(description = "Whether the account may authenticate.") boolean enabled) {
    }

    @Schema(description = "ADMIN password reset request.")
    public record PasswordUpdate(
        @Schema(description = "Replacement password accepted only for hashing and never returned.", accessMode = Schema.AccessMode.WRITE_ONLY)
        @NotBlank String newPassword) {
    }
}
