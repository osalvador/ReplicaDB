package org.replicadb.server.job.domain;

import org.replicadb.manager.ManagerCapabilities;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public final class DataSourceCapabilityCatalog {

    public DataSourceCapabilities forType(ConnectorType connectorType) {
        Objects.requireNonNull(connectorType, "connectorType must not be null");
        if (connectorType == ConnectorType.CUSTOM || connectorType.getManager() == null) {
            throw new IllegalArgumentException("Custom connector capabilities require an explicit supported scheme");
        }
        ManagerCapabilities capabilities = connectorType.getManager().getCapabilities();
        return DataSourceCapabilities.from(capabilities);
    }
}
