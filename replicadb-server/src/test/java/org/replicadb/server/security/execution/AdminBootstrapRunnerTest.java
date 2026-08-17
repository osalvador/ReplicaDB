package org.replicadb.server.security.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.security.crypto.argon2.Argon2PasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdminBootstrapRunnerTest {

    private final AppUserRepository repository = mock(AppUserRepository.class);

    @Test
    void createsEnabledAdminWithArgonHashFromEnvironment() {
        PasswordEncoder encoder = Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8();
        AtomicReference<AppUser> inserted = new AtomicReference<>();
        when(repository.countByRole(GlobalRole.ADMIN)).thenReturn(0L);
        when(repository.insert(any(AppUser.class))).thenAnswer(invocation -> {
            AppUser user = invocation.getArgument(0);
            inserted.set(user);
            return user;
        });
        AdminBootstrapRunner runner = runner(encoder, Map.of(
                "REPLICADB_BOOTSTRAP_ADMIN_USERNAME", "bootstrap-admin",
                "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", "bootstrap-password"));

        runner.run(null);

        AppUser user = inserted.get();
        assertEquals("bootstrap-admin", user.username());
        assertEquals(GlobalRole.ADMIN, user.role());
        assertTrue(user.enabled());
        assertTrue(encoder.matches("bootstrap-password", user.passwordHash()));
    }

    @Test
    void skipsBootstrapWhenAdminAlreadyExists() {
        PasswordEncoder encoder = Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8();
        when(repository.countByRole(GlobalRole.ADMIN)).thenReturn(1L);
        AdminBootstrapRunner runner = runner(encoder, Map.of(
                "REPLICADB_BOOTSTRAP_ADMIN_USERNAME", "bootstrap-admin",
                "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", "bootstrap-password"));

        runner.run(null);

        verify(repository, never()).insert(any(AppUser.class));
    }

    @Test
    void failsClosedWhenBootstrapVariablesAreMissing() {
        PasswordEncoder encoder = Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8();
        when(repository.countByRole(GlobalRole.ADMIN)).thenReturn(0L);
        AdminBootstrapRunner runner = runner(encoder, Map.of());

        assertThrows(IllegalStateException.class, () -> runner.run(null));
        verify(repository, never()).insert(any(AppUser.class));
    }

    @Test
    void acceptsDuplicateInsertWhenAnotherAdminWonTheRace() {
        PasswordEncoder encoder = Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8();
        when(repository.countByRole(GlobalRole.ADMIN)).thenReturn(0L, 1L);
        doThrow(new DuplicateKeyException("duplicate username")).when(repository).insert(any(AppUser.class));
        AdminBootstrapRunner runner = runner(encoder, Map.of(
                "REPLICADB_BOOTSTRAP_ADMIN_USERNAME", "bootstrap-admin",
                "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", "bootstrap-password"));

        runner.run(null);

        verify(repository).insert(any(AppUser.class));
    }

    @Test
    void propagatesDuplicateInsertWhenNoAdminExistsAfterRace() {
        PasswordEncoder encoder = Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8();
        when(repository.countByRole(GlobalRole.ADMIN)).thenReturn(0L, 0L);
        DuplicateKeyException failure = new DuplicateKeyException("duplicate non-admin username");
        doThrow(failure).when(repository).insert(any(AppUser.class));
        AdminBootstrapRunner runner = runner(encoder, Map.of(
                "REPLICADB_BOOTSTRAP_ADMIN_USERNAME", "bootstrap-admin",
                "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", "bootstrap-password"));

        assertThrows(DuplicateKeyException.class, () -> runner.run(null));
    }

    private AdminBootstrapRunner runner(PasswordEncoder encoder, Map<String, String> values) {
        return new AdminBootstrapRunner(repository, encoder, values::get);
    }
}
