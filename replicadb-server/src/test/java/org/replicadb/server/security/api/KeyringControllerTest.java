package org.replicadb.server.security.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.replicadb.server.security.secret.KeyEncryptionKeyProvider;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import({PostgresTestcontainersConfig.class, KeyringControllerTest.RotationConfiguration.class})
@WithMockUser(roles = "ADMIN")
class KeyringControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ManagedDataSourceRepository repository;

    @Autowired
    private SecretProtectionService protectionService;

    @Autowired
    private TwoVersionKeyProvider keyProvider;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, datasource_permission, job_definition, managed_datasource CASCADE",
                Map.of());
        keyProvider.reset();
    }

    @Test
    void returnsEmptyConvergedStatus() throws Exception {
        mockMvc.perform(get("/api/v1/keyring/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.envelopes", hasSize(0)))
                .andExpect(jsonPath("$.converged").value(true));
    }

    @Test
    void returnsCurrentVersionEnvelopeCounts() throws Exception {
        insert("current");

        mockMvc.perform(get("/api/v1/keyring/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.currentVersion").value("v1"))
                .andExpect(jsonPath("$.envelopes", hasSize(1)))
                .andExpect(jsonPath("$.envelopes[0].keyVersion").value("v1"))
                .andExpect(jsonPath("$.envelopes[0].count").value(1))
                .andExpect(jsonPath("$.envelopes[0].known").value(true))
                .andExpect(jsonPath("$.converged").value(true));
    }

    @Test
    void requiresCsrfForReencryption() throws Exception {
        mockMvc.perform(post("/api/v1/keyring/reencrypt"))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "USER")
    void requiresAdminForReencryption() throws Exception {
        mockMvc.perform(post("/api/v1/keyring/reencrypt").with(csrf()))
                .andExpect(status().isForbidden());
    }

    @Test
    void reencryptsOneEnvelopeAndRecordsAnAuditEvent() throws Exception {
        UUID id = insert("stale");
        keyProvider.rotate();

        mockMvc.perform(post("/api/v1/keyring/reencrypt")
                        .param("batchSize", "1")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reencrypted").value(1))
                .andExpect(jsonPath("$.remaining").value(0))
                .andExpect(jsonPath("$.converged").value(true));

        assertEquals("v2", repository.findById(id).orElseThrow().keyVersion());
        Long auditCount = jdbcTemplate.getJdbcTemplate().queryForObject(
                "SELECT COUNT(*) FROM audit_event WHERE action = 'KEYRING_REENCRYPTED'", Long.class);
        assertEquals(1L, auditCount);
    }

    @Test
    void clampsAnOversizedBatchWithoutReturningAnError() throws Exception {
        insert("stale");
        keyProvider.rotate();

        mockMvc.perform(post("/api/v1/keyring/reencrypt")
                        .param("batchSize", "5000")
                        .with(csrf()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reencrypted").value(1))
                .andExpect(jsonPath("$.remaining").value(0));
        assertTrue(repository.countByKeyVersion().getOrDefault("v1", 0L) <= 1L);
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
