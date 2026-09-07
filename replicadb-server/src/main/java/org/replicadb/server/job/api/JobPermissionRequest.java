package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.replicadb.server.security.domain.JobPermissionType;

import java.util.Set;

@Schema(description = "Complete replacement set of grants for one user and job.")
public record JobPermissionRequest(
	@ArraySchema(schema = @Schema(description = "Job permission.", allowableValues = {"VIEW", "EDIT", "EXECUTE", "CANCEL"}))
	@NotNull Set<JobPermissionType> permissions) {
}
