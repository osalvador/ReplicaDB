package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class ManagedDataSourceRepositoryIT {

    @Autowired
    private ManagedDataSourceRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, managed_datasource CASCADE", Map.of());
    }

    @Test
    void roundTripsCiphertextAndReturnsSafeSummaryWithoutDecrypting() {
        byte[] ciphertext = {4, 8, 15, 16, 23, 42};
        ManagedDataSource original = dataSource("round-trip", ConnectorType.POSTGRES,
                Map.of("sslmode", "require", "ApplicationName", "ReplicaDB"), ciphertext);

        ManagedDataSource inserted = repository.insert(original);
        ManagedDataSource found = repository.findById(inserted.id()).orElseThrow();
        ManagedDataSourceSummary summary = repository.findSummaryById(inserted.id()).orElseThrow();

        assertEquals(inserted.id(), found.id());
        assertEquals("round-trip", found.name());
        assertEquals(ConnectorType.POSTGRES, found.connectorType());
        assertEquals(Map.of("sslmode", "require", "ApplicationName", "ReplicaDB"), found.technicalParams());
        assertArrayEquals(ciphertext, found.encryptedSecurity());
        assertEquals("jdbc:postgresql://host/db", found.safeConnectDisplay());
        assertTrue(summary.securityConfigured());
        assertEquals(found.technicalParams(), summary.technicalParams());
        assertEquals(found.encryptionAlgorithm(), summary.encryptionAlgorithm());
        assertEquals(found.keyVersion(), summary.keyVersion());
        assertNotNull(summary.createdAt());
        assertNotNull(summary.updatedAt());
    }

    @Test
    void updatesMetadataAndCiphertextAsOneRepositoryOperation() {
        ManagedDataSource inserted = repository.insert(dataSource("before-update", ConnectorType.POSTGRES,
                Map.of("sslmode", "require"), new byte[]{1, 2}));
        jdbcTemplate.update("UPDATE managed_datasource SET updated_at = now() - interval '1 second' "
            + "WHERE id = :id", Map.of("id", inserted.id()));
        inserted = repository.findById(inserted.id()).orElseThrow();
        ManagedDataSource replacement = new ManagedDataSource(inserted.id(), "after-update", ConnectorType.MYSQL,
                "jdbc:mysql://host/db", Map.of("useSSL", "true"), new byte[]{9, 8, 7},
            2, "AES-256-GCM", "key-2", inserted.createdAt(), inserted.updatedAt());

        ManagedDataSource updated = repository.update(replacement);

        assertEquals("after-update", updated.name());
        assertEquals(ConnectorType.MYSQL, updated.connectorType());
        assertEquals(Map.of("useSSL", "true"), updated.technicalParams());
        assertArrayEquals(new byte[]{9, 8, 7}, updated.encryptedSecurity());
        assertEquals(2, updated.securityFormatVersion());
        assertEquals("key-2", updated.keyVersion());
        assertTrue(updated.updatedAt().isAfter(inserted.updatedAt()));
    }

    @Test
    void filtersAndCountsBeforeApplyingPagination() {
        repository.insert(dataSource("postgres-a", ConnectorType.POSTGRES, Map.of(), new byte[]{1}));
        ManagedDataSource mysql = repository.insert(dataSource("mysql-b", ConnectorType.MYSQL,
                Map.of(), new byte[]{2}));
        ManagedDataSource postgres = repository.insert(dataSource("postgres-c", ConnectorType.POSTGRES,
                Map.of(), new byte[]{3}));

        assertEquals(2, repository.count(null, Set.of(ConnectorType.POSTGRES)));
        assertEquals(1, repository.findPage(0, 1, Set.of(mysql.id(), postgres.id()),
                Set.of(ConnectorType.POSTGRES)).size());
        assertEquals("postgres-c", repository.findPage(0, 10, Set.of(mysql.id(), postgres.id()),
                Set.of(ConnectorType.POSTGRES)).get(0).name());
        assertEquals(0, repository.count(Set.of(), null));
        assertTrue(repository.findPage(0, 10, Set.of(), null).isEmpty());
    }

    @Test
    void restrictsDeletionWhenAJobReferencesTheDatasource() {
        ManagedDataSource dataSource = repository.insert(dataSource("referenced", ConnectorType.POSTGRES,
                Map.of(), new byte[]{1}));
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO job_definition (
                    id, name, source_datasource_id, sink_datasource_id,
                    source_table, sink_table, mode, jobs
                ) VALUES (:id, :name, :datasourceId, :datasourceId,
                    'source_table', 'sink_table', 'complete', 1)
                """, Map.of("id", jobId, "name", "referencing-job", "datasourceId", dataSource.id()));

        assertEquals(1, repository.countJobReferences(dataSource.id()));
        assertEquals(ManagedDataSourceStore.DeleteResult.REFERENCED, repository.delete(dataSource.id()));
        assertTrue(repository.findById(dataSource.id()).isPresent());
    }

    @Test
    void deletesUnreferencedDatasourceAndReportsUnknownId() {
        ManagedDataSource dataSource = repository.insert(dataSource("deletable", ConnectorType.POSTGRES,
                Map.of(), new byte[]{1}));

        assertEquals(ManagedDataSourceStore.DeleteResult.DELETED, repository.delete(dataSource.id()));
        assertEquals(ManagedDataSourceStore.DeleteResult.NOT_FOUND, repository.delete(dataSource.id()));
        assertFalse(repository.findById(dataSource.id()).isPresent());
    }

    @Test
    void databaseForeignKeyRemainsTheFinalDeleteAuthority() {
        ManagedDataSource dataSource = repository.insert(dataSource("constraint", ConnectorType.POSTGRES,
                Map.of(), new byte[]{1}));
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO job_definition (
                    id, name, source_datasource_id, sink_datasource_id,
                    source_table, sink_table, mode, jobs
                ) VALUES (:id, :name, :datasourceId, :datasourceId,
                    'source_table', 'sink_table', 'complete', 1)
                """, Map.of("id", jobId, "name", "constraint-job", "datasourceId", dataSource.id()));

        try {
            jdbcTemplate.update("DELETE FROM managed_datasource WHERE id = :id", Map.of("id", dataSource.id()));
        } catch (DataIntegrityViolationException expected) {
            assertTrue(repository.findById(dataSource.id()).isPresent());
            return;
        }
        throw new AssertionError("Datasource foreign key did not restrict deletion");
    }

    private static ManagedDataSource dataSource(String name, ConnectorType connectorType,
                                                Map<String, String> technicalParams, byte[] ciphertext) {
        String connect = connectorType == ConnectorType.POSTGRES
            ? "jdbc:postgresql://host/db"
            : "jdbc:" + connectorType.getWireValue() + "://host/db";
        return new ManagedDataSource(UUID.randomUUID(), name, connectorType,
            connect, technicalParams, ciphertext,
                1, "AES-256-GCM", "key-1", null, null);
    }
}
