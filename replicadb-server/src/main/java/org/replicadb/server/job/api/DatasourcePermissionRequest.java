package org.replicadb.server.job.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotNull;
import org.replicadb.server.security.domain.DataSourcePermissionType;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = false)
public record DatasourcePermissionRequest(@NotNull Set<DataSourcePermissionType> permissions) {

    public DatasourcePermissionRequest {
        permissions = permissions == null ? Set.of() : Set.copyOf(permissions);
    }
}
