package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.security.domain.DataSourcePermissionType;

import java.util.Set;
import java.util.UUID;

@Schema(description = "Datasource grants grouped for one user.")
public record DatasourcePermissionResponse(
        @Schema(description = "Granted user identifier.", format = "uuid") UUID userId,
        @Schema(description = "Granted user's current username.") String username,
        @ArraySchema(schema = @Schema(description = "Granted datasource permission.", allowableValues = {"VIEW", "USE", "EDIT"}))
        Set<DataSourcePermissionType> permissions) {
}
