package org.replicadb.server.job.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = false)
@Schema(description = "Request for a managed datasource profile. Unknown fields are rejected.")
public record DatasourceRequest(
    @Schema(description = "Unique display name.", example = "Warehouse source")
    @NotBlank String name,
    @Schema(description = "Registered connector wire name.", example = "postgres")
    @NotBlank String connectorType,
    @Schema(description = "Non-secret connector settings. Omitted values default to an empty map.")
    Map<String, String> technicalParams,
    @Schema(description = "Security values to encrypt. On update, omitted or blank entries preserve stored values.", accessMode = Schema.AccessMode.WRITE_ONLY)
    Map<String, String> security,
    @Schema(description = "Security keys to remove explicitly during an update.", example = "[\"password\"]")
    Set<String> clearSecurityKeys) {

    public DatasourceRequest {
        technicalParams = technicalParams == null ? Map.of() : Map.copyOf(technicalParams);
        security = security == null ? Map.of() : Map.copyOf(security);
        clearSecurityKeys = clearSecurityKeys == null ? Set.of() : Set.copyOf(clearSecurityKeys);
    }

    @JsonAnySetter
    public void rejectUnknownProperty(String property, Object value) {
        throw new IllegalArgumentException("Unknown datasource request field: " + property);
    }
}
