package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.security.domain.JobPermissionType;

import java.util.Set;
import java.util.UUID;

@Schema(description = "Job grants grouped for one user.")
public record JobPermissionResponse(
	@Schema(description = "Granted user identifier.", format = "uuid") UUID userId,
	@Schema(description = "Granted user's current username.") String username,
	@ArraySchema(schema = @Schema(description = "Granted job permission.", allowableValues = {"VIEW", "EDIT", "EXECUTE", "CANCEL"}))
	Set<JobPermissionType> permissions) {
}
