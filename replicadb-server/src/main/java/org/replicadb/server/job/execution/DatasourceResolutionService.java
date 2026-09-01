package org.replicadb.server.job.execution;

import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.DataSourceSecurityKeyPolicy;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ResolvedDataSource;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Service
@Profile({"api", "worker"})
public final class DatasourceResolutionService {

    private final SecretProtectionService protectionService;

    public DatasourceResolutionService(SecretProtectionService protectionService) {
        this.protectionService = Objects.requireNonNull(protectionService,
                "protectionService must not be null");
    }

    public ResolvedJobDefinition resolve(ClaimedRunPreparation preparation) {
        Objects.requireNonNull(preparation, "preparation must not be null");
        Map<java.util.UUID, ResolvedDataSource> resolved = new LinkedHashMap<>();
        ResolvedDataSource source = resolve(preparation.sourceDataSource(), resolved);
        ResolvedDataSource sink = resolve(preparation.sinkDataSource(), resolved);
        return new ResolvedJobDefinition(preparation.definition(), source, sink);
    }

    private ResolvedDataSource resolve(ManagedDataSource dataSource,
                                       Map<java.util.UUID, ResolvedDataSource> resolved) {
        ResolvedDataSource existing = resolved.get(dataSource.id());
        if (existing != null) {
            return existing;
        }
        Map<String, String> security = protectionService.decrypt(dataSource.id(),
                protectionService.deserialize(dataSource.encryptedSecurity()));
        DataSourceSecurityKeyPolicy.validateSecurityParameters(security);
        String connect = required(security, "connect");
        ResolvedDataSource value = new ResolvedDataSource(dataSource.id(), dataSource.name(),
                dataSource.connectorType(), connect, security.get("user"), security.get("password"),
                authentication(security), dataSource.technicalParams(), security);
        resolved.put(dataSource.id(), value);
        return value;
    }

    private static AzureAuthentication authentication(Map<String, String> security) {
        return new AzureAuthentication(security.get("auth.mode"), security.get("auth.principal.id"),
                security.get("auth.login.hint"), security.get("auth.client.certificate"),
                security.get("auth.client.key"));
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Datasource security must contain " + key);
        }
        return value;
    }
}
