package org.replicadb.server.security.secret;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import({PostgresTestcontainersConfig.class, KeyringRowReencryptorTest.RotationConfiguration.class})
class KeyringRowReencryptorTest {

    @Autowired
    private KeyringRowReencryptor reencryptor;

    @Autowired
    private ManagedDataSourceRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private SecretProtectionService protectionService;

    @Autowired
    private TwoVersionKeyProvider keyProvider;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, job_definition, managed_datasource CASCADE",
                Map.of());
        keyProvider.reset();
    }

    @Test
    void reencryptsAnOldEnvelopeWithTheCurrentKey() {
        UUID id = UUID.randomUUID();
        EncryptedSecurityBundle original = protectionService.encrypt(id, Map.of("password", "placeholder"));
        insert(id, "old", original);
        keyProvider.rotate();

        assertEquals(Optional.of(id), reencryptor.reencryptOne("v2", keyProvider.knownVersions()));

        ManagedDataSource updated = repository.findById(id).orElseThrow();
        EncryptedSecurityBundle rotated = protectionService.deserialize(updated.encryptedSecurity());
        assertEquals("v2", updated.keyVersion());
        assertEquals(Map.of("password", "placeholder"), protectionService.decrypt(id, rotated));
    }

    @Test
    void leavesCurrentEnvelopeUntouched() {
        UUID id = UUID.randomUUID();
        EncryptedSecurityBundle current = protectionService.encrypt(id, Map.of("password", "placeholder"));
        insert(id, "current", current);
        Instant updatedAt = repository.findById(id).orElseThrow().updatedAt();

        assertTrue(reencryptor.reencryptOne("v1", Set.of("v1", "v2")).isEmpty());

        assertEquals(updatedAt, repository.findById(id).orElseThrow().updatedAt());
    }

    @Test
    void leavesUnknownEnvelopeVersionUntouched() {
        UUID id = UUID.randomUUID();
        EncryptedSecurityBundle original = protectionService.encrypt(id, Map.of("password", "placeholder"));
        insert(id, "unknown", original);
        jdbcTemplate.update("UPDATE managed_datasource SET key_version = :version WHERE id = :id",
                Map.of("version", "orphaned", "id", id));
        Instant updatedAt = repository.findById(id).orElseThrow().updatedAt();

        assertTrue(reencryptor.reencryptOne("v2", Set.of("v1", "v2")).isEmpty());

        ManagedDataSource unchanged = repository.findById(id).orElseThrow();
        assertEquals("orphaned", unchanged.keyVersion());
        assertEquals(updatedAt, unchanged.updatedAt());
    }

    private void insert(UUID id, String name, EncryptedSecurityBundle bundle) {
        repository.insert(new ManagedDataSource(id, name, ConnectorType.POSTGRES,
                "jdbc:postgresql://[REDACTED]/db", Map.of(), protectionService.serialize(bundle),
                bundle.formatVersion(), bundle.algorithm(), bundle.keyVersion(), null, null));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class RotationConfiguration {

        @Bean
        @Primary
        TwoVersionKeyProvider keyEncryptionKeyProvider() throws Exception {
            return new TwoVersionKeyProvider();
        }
    }

    static final class TwoVersionKeyProvider implements KeyEncryptionKeyProvider {

        private final KeyEncryptionKey first;
        private final KeyEncryptionKey second;
        private volatile boolean rotated;

        TwoVersionKeyProvider() throws Exception {
            first = new KeyEncryptionKey("v1", key());
            second = new KeyEncryptionKey("v2", key());
        }

        @Override
        public KeyEncryptionKey current() {
            return rotated ? second : first;
        }

        @Override
        public Optional<KeyEncryptionKey> find(String version) {
            if (first.version().equals(version)) {
                return Optional.of(first);
            }
            if (second.version().equals(version)) {
                return Optional.of(second);
            }
            return Optional.empty();
        }

        @Override
        public Set<String> knownVersions() {
            return Set.of(first.version(), second.version());
        }

        void rotate() {
            rotated = true;
        }

        void reset() {
            rotated = false;
        }

        private static SecretKey key() throws Exception {
            KeyGenerator generator = KeyGenerator.getInstance("AES");
            generator.init(256);
            return generator.generateKey();
        }
    }
}
