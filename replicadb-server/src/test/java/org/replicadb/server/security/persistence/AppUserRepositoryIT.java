package org.replicadb.server.security.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class AppUserRepositoryIT {

    @Autowired
    private AppUserRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE app_user CASCADE", Map.of());
    }

    @Test
    void insertsAndReadsByIdAndUsername() {
        AppUser inserted = repository.insert(user("admin-user", GlobalRole.ADMIN));

        assertEquals(inserted, repository.findById(inserted.id()).orElseThrow());
        assertEquals(inserted, repository.findByUsername(inserted.username()).orElseThrow());
        assertNotNull(inserted.id());
        assertNotNull(inserted.createdAt());
        assertNotNull(inserted.updatedAt());
    }

    @Test
    void returnsEmptyForUnknownUsername() {
        assertTrue(repository.findByUsername("missing-user").isEmpty());
    }

    @Test
    void rejectsDuplicateUsername() {
        AppUser user = user("duplicate-user", GlobalRole.VIEWER);
        repository.insert(user);

        assertThrows(DuplicateKeyException.class, () -> repository.insert(user));
    }

    @Test
    void updatesMutableFieldsAndRefreshesTimestamp() {
        AppUser inserted = repository.insert(user("update-user", GlobalRole.OPERATOR));
        jdbcTemplate.update("UPDATE app_user SET updated_at = now() - interval '1 second' WHERE id = :id",
                Map.of("id", inserted.id()));
        AppUser stale = repository.findById(inserted.id()).orElseThrow();
        AppUser replacement = new AppUser(inserted.id(), inserted.username(), "new-password-hash",
                GlobalRole.ADMIN, false, stale.createdAt(), stale.updatedAt());

        AppUser updated = repository.update(replacement);

        assertEquals("new-password-hash", updated.passwordHash());
        assertEquals(GlobalRole.ADMIN, updated.role());
        assertFalse(updated.enabled());
        assertEquals(inserted.username(), updated.username());
        assertEquals(inserted.createdAt(), updated.createdAt());
        assertTrue(updated.updatedAt().isAfter(stale.updatedAt()));
    }

    @Test
    void rejectsUpdateForUnknownUser() {
        AppUser unknown = user("unknown-user", GlobalRole.VIEWER);

        assertThrows(java.util.NoSuchElementException.class, () -> repository.update(unknown));
    }

    @Test
    void countsByRole() {
        repository.insert(user("admin-one", GlobalRole.ADMIN));
        repository.insert(user("admin-two", GlobalRole.ADMIN));
        repository.insert(user("viewer-one", GlobalRole.VIEWER));

        assertEquals(3, repository.count());
        assertEquals(2, repository.countByRole(GlobalRole.ADMIN));
        assertEquals(1, repository.countByRole(GlobalRole.VIEWER));
        assertEquals(0, repository.countByRole(GlobalRole.OPERATOR));
    }

    @Test
    void paginatesInUsernameAndIdOrder() {
        repository.insert(user("page-c", GlobalRole.VIEWER));
        repository.insert(user("page-a", GlobalRole.VIEWER));
        repository.insert(user("page-e", GlobalRole.VIEWER));
        repository.insert(user("page-b", GlobalRole.VIEWER));
        repository.insert(user("page-d", GlobalRole.VIEWER));

        assertEquals(java.util.List.of("page-a", "page-b"),
                repository.findPage(0, 2).stream().map(AppUser::username).toList());
        assertEquals(java.util.List.of("page-c", "page-d"),
                repository.findPage(1, 2).stream().map(AppUser::username).toList());
        assertEquals(java.util.List.of("page-e"),
                repository.findPage(2, 2).stream().map(AppUser::username).toList());
    }

    private static AppUser user(String username, GlobalRole role) {
        return new AppUser(null, username, "password-hash", role, true, null, null);
    }
}
