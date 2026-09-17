package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class ManagedDataSourceRepositoryTest {

    @Autowired
    private ManagedDataSourceRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private ExecutorService executor;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, job_definition, managed_datasource CASCADE",
                Map.of());
    }

    @AfterEach
    void stopExecutor() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void countsDatasourceEnvelopesByKeyVersion() {
        ManagedDataSource first = insert("first");
        ManagedDataSource second = insert("second");
        ManagedDataSource third = insert("third");
        setKeyVersion(third.id(), "other");

        assertEquals(Map.of("test", 2L, "other", 1L), repository.countByKeyVersion());
        assertEquals("test", first.keyVersion());
        assertEquals("test", second.keyVersion());
    }

    @Test
    void findsKnownDatasourceEnvelopePendingReencryption() {
        insert("current");
        ManagedDataSource pending = insert("pending");
        setKeyVersion(pending.id(), "other");

        assertEquals(Optional.of(pending.id()),
                repository.findIdPendingReencryption("test", Set.of("test", "other")));
        assertTrue(repository.findIdPendingReencryption("test", Set.of("test")).isEmpty());
    }

    @Test
    void excludesUnknownDatasourceEnvelopeVersions() {
        ManagedDataSource orphan = insert("orphan");
        setKeyVersion(orphan.id(), "orphaned");

        assertTrue(repository.findIdPendingReencryption("test", Set.of("test", "other")).isEmpty());
    }

    @Test
    void skipsRowsLockedByOtherTransactions() throws Exception {
        ManagedDataSource first = insert("first");
        ManagedDataSource second = insert("second");
        setKeyVersion(first.id(), "other");
        setKeyVersion(second.id(), "other");
        Set<String> knownVersions = Set.of("test", "other");
        CountDownLatch firstLocked = new CountDownLatch(1);
        CountDownLatch secondLocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        executor = Executors.newFixedThreadPool(2);

        Future<UUID> firstSelection = executor.submit(() -> holdSelection(
                firstLocked, release, knownVersions));
        assertTrue(firstLocked.await(10, TimeUnit.SECONDS));
        Future<UUID> secondSelection = executor.submit(() -> holdSelection(
                secondLocked, release, knownVersions));
        assertTrue(secondLocked.await(10, TimeUnit.SECONDS));

        Optional<UUID> thirdSelection = new TransactionTemplate(transactionManager).execute(status ->
                repository.findIdPendingReencryption("test", knownVersions));

        assertTrue(thirdSelection.isEmpty());
        release.countDown();
        UUID firstId = firstSelection.get(10, TimeUnit.SECONDS);
        UUID secondId = secondSelection.get(10, TimeUnit.SECONDS);
        assertNotNull(firstId);
        assertNotNull(secondId);
        assertNotEquals(firstId, secondId);
    }

    private UUID holdSelection(CountDownLatch selected, CountDownLatch release,
                                Set<String> knownVersions) {
        return new TransactionTemplate(transactionManager).execute(status -> {
            UUID id = repository.findIdPendingReencryption("test", knownVersions).orElseThrow();
            selected.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    status.setRollbackOnly();
                    throw new IllegalStateException("Timed out waiting to release row lock");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                status.setRollbackOnly();
                throw new IllegalStateException("Interrupted while holding row lock", exception);
            }
            return id;
        });
    }

    private ManagedDataSource insert(String name) {
        ManagedDataSource dataSource = new ManagedDataSource(UUID.randomUUID(), name, ConnectorType.POSTGRES,
                "jdbc:postgresql://[REDACTED]/db", Map.of(), new byte[]{1}, 1, "AES-256-GCM", "test",
                null, null);
        return repository.insert(dataSource);
    }

    private void setKeyVersion(UUID id, String version) {
        jdbcTemplate.update("UPDATE managed_datasource SET key_version = :version WHERE id = :id",
                Map.of("version", version, "id", id));
    }
}
