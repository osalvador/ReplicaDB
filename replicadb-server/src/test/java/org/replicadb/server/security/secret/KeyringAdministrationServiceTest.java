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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import({PostgresTestcontainersConfig.class, KeyringAdministrationServiceTest.RotationConfiguration.class})
class KeyringAdministrationServiceTest {

    @Autowired
    private KeyringAdministrationService service;

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
    void reportsKnownPendingAndUnknownEnvelopeVersions() {
        UUID old = insert("old");
        keyProvider.rotate();
        UUID current = insert("current");
        UUID unknown = insert("unknown");
        jdbcTemplate.update("UPDATE managed_datasource SET key_version = :version WHERE id = :id",
                Map.of("version", "orphaned", "id", unknown));

        KeyringAdministrationService.KeyringStatus status = service.status();

        assertEquals(Set.of("v1", "v2"), status.knownVersions());
        assertEquals("v2", status.currentVersion());
        assertEquals(1, status.reencryptionRequired());
        assertEquals(1, status.unknownVersionCount());
        assertFalse(status.converged());
        assertEquals("v1", repository.findById(old).orElseThrow().keyVersion());
        assertEquals("v2", repository.findById(current).orElseThrow().keyVersion());
    }

    @Test
    void reportsConvergedWhenAllEnvelopesUseTheCurrentVersion() {
        insert("current");

        KeyringAdministrationService.KeyringStatus status = service.status();

        assertTrue(status.converged());
        assertEquals(0, status.reencryptionRequired());
        assertEquals(0, status.unknownVersionCount());
    }

    @Test
    void processesOnlyTheRequestedBatch() {
        for (int index = 0; index < 5; index++) {
            insert("pending-" + index);
        }
        keyProvider.rotate();

        KeyringAdministrationService.ReencryptResult result = service.reencrypt(2);

        assertEquals(2, result.reencrypted());
        assertEquals(3, result.remaining());
    }

    @Test
    void stopsWhenTheDatasourceStoreIsConverged() {
        for (int index = 0; index < 3; index++) {
            insert("pending-" + index);
        }
        keyProvider.rotate();

        KeyringAdministrationService.ReencryptResult result = service.reencrypt(10);

        assertEquals(3, result.reencrypted());
        assertEquals(0, result.remaining());
    }

    @Test
    void repeatedReencryptionIsIdempotent() {
        insert("pending");
        keyProvider.rotate();

        service.reencrypt(10);
        KeyringAdministrationService.ReencryptResult second = service.reencrypt(10);

        assertEquals(0, second.reencrypted());
        assertEquals(0, second.remaining());
    }

    private UUID insert(String name) {
        UUID id = UUID.randomUUID();
        EncryptedSecurityBundle bundle = protectionService.encrypt(id, Map.of("password", "placeholder"));
        repository.insert(new ManagedDataSource(id, name, ConnectorType.POSTGRES,
                "jdbc:postgresql://[REDACTED]/db", Map.of(), protectionService.serialize(bundle),
                bundle.formatVersion(), bundle.algorithm(), bundle.keyVersion(), null, null));
        return id;
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
