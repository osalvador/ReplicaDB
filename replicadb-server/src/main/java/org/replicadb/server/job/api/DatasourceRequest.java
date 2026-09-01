package org.replicadb.server.job.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = false)
public record DatasourceRequest(
        @NotBlank String name,
        @NotBlank String connectorType,
        Map<String, String> technicalParams,
        Map<String, String> security,
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
