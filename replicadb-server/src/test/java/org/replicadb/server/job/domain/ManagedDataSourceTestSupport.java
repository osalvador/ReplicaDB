package org.replicadb.server.job.domain;

import org.replicadb.config.CredentialRedactor;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.replicadb.server.security.secret.SecretProtectionService;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public final class ManagedDataSourceTestSupport {

    private ManagedDataSourceTestSupport() {
    }

    public static UUID insert(ManagedDataSourceRepository repository,
                              SecretProtectionService protectionService,
                              String name, ConnectorType connectorType, String connect) {
        return insert(repository, protectionService, name, connectorType, connect, Map.of(), Map.of());
    }

    public static UUID insert(ManagedDataSourceRepository repository,
                              SecretProtectionService protectionService,
                              String name, ConnectorType connectorType, String connect,
                              Map<String, String> technicalParams,
                              Map<String, String> additionalSecurity) {
        UUID id = UUID.randomUUID();
        Map<String, String> security = new LinkedHashMap<>();
        security.put("connect", connect);
        security.putAll(additionalSecurity);
        EncryptedSecurityBundle bundle = protectionService.encrypt(id, security);
        repository.insert(new ManagedDataSource(id, name, connectorType,
                CredentialRedactor.redactConnectionString(connect), technicalParams,
                protectionService.serialize(bundle), bundle.formatVersion(), bundle.algorithm(),
                bundle.keyVersion(), Instant.now(), Instant.now()));
        return id;
    }
}
