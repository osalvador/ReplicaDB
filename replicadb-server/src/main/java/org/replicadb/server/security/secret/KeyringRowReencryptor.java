package org.replicadb.server.security.secret;

import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Component
public class KeyringRowReencryptor {

    private final ManagedDataSourceStore repository;
    private final SecretProtectionService protectionService;

    public KeyringRowReencryptor(ManagedDataSourceStore repository,
                                 SecretProtectionService protectionService) {
        this.repository = repository;
        this.protectionService = protectionService;
    }

    @Transactional
    public Optional<UUID> reencryptOne(String currentVersion, Set<String> knownVersions) {
        Optional<UUID> candidate = repository.findIdPendingReencryption(currentVersion, knownVersions);
        if (candidate.isEmpty()) {
            return Optional.empty();
        }

        UUID id = candidate.orElseThrow();
        ManagedDataSource existing = repository.findByIdForUpdate(id)
                .orElseThrow(() -> new IllegalStateException("ManagedDataSource not found: " + id));
        EncryptedSecurityBundle rotated = protectionService.reencrypt(id,
                protectionService.deserialize(existing.encryptedSecurity()));
        ManagedDataSource replacement = new ManagedDataSource(existing.id(), existing.name(),
                existing.connectorType(), existing.safeConnectDisplay(), existing.technicalParams(),
                protectionService.serialize(rotated), rotated.formatVersion(), rotated.algorithm(),
                rotated.keyVersion(), existing.createdAt(), existing.updatedAt());
        repository.update(replacement);
        return Optional.of(id);
    }
}
