package org.replicadb.server.job.domain;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record ResolvedDataSource(
        UUID id,
        String name,
        ConnectorType connectorType,
        String connect,
        String user,
        String password,
        AzureAuthentication authentication,
        Map<String, String> technicalParams,
        Map<String, String> securityParams) {

    public ResolvedDataSource {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(connectorType, "connectorType must not be null");
        if (connect == null || connect.isBlank()) {
            throw new IllegalArgumentException("connect must not be blank");
        }
        technicalParams = technicalParams == null ? Map.of() : Map.copyOf(technicalParams);
        securityParams = securityParams == null ? Map.of() : Map.copyOf(securityParams);
    }

    public ResolvedDataSource(UUID id, String name, ConnectorType connectorType, String connect,
                              String user, String password, AzureAuthentication authentication,
                              Map<String, String> technicalParams) {
        this(id, name, connectorType, connect, user, password, authentication, technicalParams, Map.of());
    }
}
