package org.replicadb.server.security.api;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

@Schema(description = "Credentials used only to create a server-owned session.")
public record LoginRequest(
        @Schema(description = "ReplicaDB username.", example = "api-operator") @NotBlank String username,
        @Schema(description = "Password accepted only for authentication and never returned.", accessMode = Schema.AccessMode.WRITE_ONLY)
        @NotBlank String password) {
}
