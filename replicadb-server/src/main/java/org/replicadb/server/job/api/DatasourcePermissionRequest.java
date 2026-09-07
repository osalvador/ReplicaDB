package org.replicadb.server.job.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.replicadb.server.security.domain.DataSourcePermissionType;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = false)
@Schema(description = "Complete replacement set of grants for one user and datasource.")
public record DatasourcePermissionRequest(
    @ArraySchema(schema = @Schema(description = "Datasource permission.", allowableValues = {"VIEW", "USE", "EDIT"}))
    @NotNull Set<DataSourcePermissionType> permissions) {

    public DatasourcePermissionRequest {
        permissions = permissions == null ? Set.of() : Set.copyOf(permissions);
    }
}
