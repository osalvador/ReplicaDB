package org.replicadb.server.job.api;

import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.DataSourceCapabilities;
import org.replicadb.server.job.domain.DataSourceSecurityKeyPolicy;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

@Component
@Profile("api")
public final class DatasourceMapper {

    public ConnectorType connectorType(DatasourceRequest request) {
        return ConnectorType.fromWireValue(request.connectorType());
    }

    public Map<String, String> technicalParams(DatasourceRequest request) {
        DataSourceSecurityKeyPolicy.validateTechnicalParameters(request.technicalParams());
        return request.technicalParams();
    }

    public Map<String, String> mergedSecurity(DatasourceRequest request,
                                              Map<String, String> existingSecurity) {
        return DataSourceSecurityKeyPolicy.mergeSecurityParameters(existingSecurity,
                request.security(), request.clearSecurityKeys());
    }

        public ManagedDataSource toDataSource(java.util.UUID id, DatasourceRequest request,
                          Map<String, String> security,
                          EncryptedSecurityBundle bundle,
                          byte[] serializedBundle,
                          Instant createdAt, Instant updatedAt) {
        DataSourceSecurityKeyPolicy.validateTechnicalParameters(request.technicalParams());
        DataSourceSecurityKeyPolicy.validateSecurityParameters(security);
        return new ManagedDataSource(id, request.name(), connectorType(request),
            org.replicadb.config.CredentialRedactor.redactConnectionString(security.get("connect")),
            request.technicalParams(), serializedBundle, bundle.formatVersion(), bundle.algorithm(),
            bundle.keyVersion(), createdAt, updatedAt);
        }

        public DatasourceResponse toResponse(ManagedDataSource dataSource,
                         DataSourceCapabilities capabilities,
                         boolean canView, boolean canUse, boolean canEdit) {
        return new DatasourceResponse(dataSource.id(), dataSource.name(),
            dataSource.connectorType().getWireValue(), dataSource.safeConnectDisplay(),
            dataSource.technicalParams(), dataSource.encryptedSecurity().length > 0,
            capabilitiesResponse(capabilities), canView, canUse, canEdit,
            dataSource.createdAt(), dataSource.updatedAt());
        }

        public DatasourceResponse toResponse(ManagedDataSourceSummary dataSource,
                         DataSourceCapabilities capabilities,
                         boolean canView, boolean canUse, boolean canEdit) {
        return new DatasourceResponse(dataSource.id(), dataSource.name(),
            dataSource.connectorType().getWireValue(), dataSource.safeConnectDisplay(),
            dataSource.technicalParams(), dataSource.securityConfigured(),
            capabilitiesResponse(capabilities), canView, canUse, canEdit,
            dataSource.createdAt(), dataSource.updatedAt());
        }

        private static DatasourceCapabilitiesResponse capabilitiesResponse(DataSourceCapabilities capabilities) {
        return new DatasourceCapabilitiesResponse(
            capabilities.supportsSource(),
            capabilities.supportsSink(),
            modes(capabilities.sourceModes()),
            modes(capabilities.sinkModes()),
            capabilities.sourceQuery(),
            capabilities.singleJobOnly());
        }

        private static Set<String> modes(Set<org.replicadb.cli.ReplicationMode> modes) {
        return modes.stream()
            .map(org.replicadb.cli.ReplicationMode::getModeText)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
        }
}
