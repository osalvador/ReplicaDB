package org.replicadb.server.security.secret;

import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Service
public class KeyringAdministrationService {

    private final ManagedDataSourceStore repository;
    private final KeyEncryptionKeyProvider keyProvider;
    private final KeyringRowReencryptor rowReencryptor;

    public KeyringAdministrationService(ManagedDataSourceStore repository,
                                        KeyEncryptionKeyProvider keyProvider,
                                        KeyringRowReencryptor rowReencryptor) {
        this.repository = repository;
        this.keyProvider = keyProvider;
        this.rowReencryptor = rowReencryptor;
    }

    public KeyringStatus status() {
        Set<String> knownVersions = Set.copyOf(keyProvider.knownVersions());
        String currentVersion = keyProvider.current().version();
        List<KeyringEnvelopeCount> envelopes = envelopeCounts(repository.countByKeyVersion(), knownVersions);
        long reencryptionRequired = envelopes.stream()
                .filter(envelope -> envelope.known() && !currentVersion.equals(envelope.keyVersion()))
                .mapToLong(KeyringEnvelopeCount::count)
                .sum();
        long unknownVersionCount = envelopes.stream()
                .filter(envelope -> !envelope.known())
                .mapToLong(KeyringEnvelopeCount::count)
                .sum();
        boolean converged = envelopes.stream()
                .allMatch(envelope -> currentVersion.equals(envelope.keyVersion()));
        return new KeyringStatus(knownVersions, currentVersion, envelopes, reencryptionRequired,
                unknownVersionCount, converged);
    }

    public ReencryptResult reencrypt(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        String currentVersion = keyProvider.current().version();
        Set<String> knownVersions = Set.copyOf(keyProvider.knownVersions());
        int processed = 0;
        while (processed < batchSize) {
            Optional<?> result = rowReencryptor.reencryptOne(currentVersion, knownVersions);
            if (result.isEmpty()) {
                break;
            }
            processed++;
        }
        long remaining = repository.countByKeyVersion().entrySet().stream()
                .filter(entry -> !currentVersion.equals(entry.getKey()))
                .mapToLong(Map.Entry::getValue)
                .sum();
        return new ReencryptResult(processed, remaining);
    }

    private static List<KeyringEnvelopeCount> envelopeCounts(Map<String, Long> counts,
                                                              Set<String> knownVersions) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new KeyringEnvelopeCount(entry.getKey(), entry.getValue(),
                        knownVersions.contains(entry.getKey())))
                .toList();
    }

    public record KeyringEnvelopeCount(String keyVersion, long count, boolean known) {
    }

    public record KeyringStatus(Set<String> knownVersions, String currentVersion,
                                List<KeyringEnvelopeCount> envelopes, long reencryptionRequired,
                                long unknownVersionCount, boolean converged) {
    }

    public record ReencryptResult(int reencrypted, long remaining) {
    }
}
