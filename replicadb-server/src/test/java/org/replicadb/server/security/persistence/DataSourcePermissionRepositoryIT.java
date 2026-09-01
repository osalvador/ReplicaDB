package org.replicadb.server.security.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.replicadb.server.security.domain.DataSourcePermissionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class DataSourcePermissionRepositoryIT {

    @Autowired
    private DataSourcePermissionRepository repository;

    @Autowired
    private ManagedDataSourceRepository dataSourceRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, managed_datasource CASCADE", Map.of());
    }

    @Test
    void replacesAndChecksPermissionsIdempotently() {
        ManagedDataSource dataSource = dataSourceRepository.insert(dataSource("permission-source"));
        UUID userId = insertUser("datasource-user");

        repository.grant(dataSource.id(), userId, DataSourcePermissionType.VIEW);
        repository.grant(dataSource.id(), userId, DataSourcePermissionType.VIEW);
        assertTrue(repository.hasPermission(dataSource.id(), userId, DataSourcePermissionType.VIEW));
        assertFalse(repository.hasPermission(dataSource.id(), userId, DataSourcePermissionType.EDIT));

        repository.replace(dataSource.id(), userId,
                Set.of(DataSourcePermissionType.USE, DataSourcePermissionType.EDIT));

        assertFalse(repository.hasPermission(dataSource.id(), userId, DataSourcePermissionType.VIEW));
        assertTrue(repository.hasPermission(dataSource.id(), userId, DataSourcePermissionType.USE));
        assertTrue(repository.hasPermission(dataSource.id(), userId, DataSourcePermissionType.EDIT));
        assertEquals(2, repository.findByDatasourceId(dataSource.id()).size());
    }

    @Test
    void findsDatasourceIdsByPermissionAndRevokesIndividually() {
        ManagedDataSource first = dataSourceRepository.insert(dataSource("permission-first"));
        ManagedDataSource second = dataSourceRepository.insert(dataSource("permission-second"));
        UUID userId = insertUser("datasource-visible-user");

        repository.grant(first.id(), userId, DataSourcePermissionType.VIEW);
        repository.grant(second.id(), userId, DataSourcePermissionType.USE);

        assertEquals(Set.of(first.id()), repository.findDatasourceIdsWithPermission(
                userId, DataSourcePermissionType.VIEW));
        assertEquals(Set.of(second.id()), repository.findDatasourceIdsWithPermission(
                userId, DataSourcePermissionType.USE));

        repository.revoke(first.id(), userId, DataSourcePermissionType.VIEW);
        repository.revoke(first.id(), userId, DataSourcePermissionType.VIEW);
        assertTrue(repository.findByDatasourceId(first.id()).isEmpty());
    }

    @Test
    void cascadesPermissionsWhenDatasourceIsDeleted() {
        ManagedDataSource dataSource = dataSourceRepository.insert(dataSource("permission-delete"));
        UUID userId = insertUser("datasource-delete-user");
        repository.grant(dataSource.id(), userId, DataSourcePermissionType.VIEW);

        assertEquals(ManagedDataSourceStore.DeleteResult.DELETED, dataSourceRepository.delete(dataSource.id()));
        assertTrue(repository.findByDatasourceId(dataSource.id()).isEmpty());
    }

    private UUID insertUser(String username) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO app_user (id, username, password_hash, role, enabled)
                VALUES (:id, :username, 'test-hash', 'VIEWER', true)
                """, Map.of("id", id, "username", username));
        return id;
    }

    private static ManagedDataSource dataSource(String name) {
        return new ManagedDataSource(UUID.randomUUID(), name, ConnectorType.POSTGRES,
                "jdbc:postgresql://host/db", Map.of("sslmode", "require"), new byte[]{1, 2, 3},
                1, "AES-256-GCM", "key-1", null, null);
    }
}
