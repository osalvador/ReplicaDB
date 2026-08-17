package org.replicadb.server.security.auth;

import org.junit.jupiter.api.Test;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReplicaDbUserDetailsServiceTest {

    private final AppUserRepository repository = mock(AppUserRepository.class);
    private final ReplicaDbUserDetailsService service = new ReplicaDbUserDetailsService(repository);

    @Test
    void loadsEnabledUserWithRoleAuthority() {
        AppUser user = user(GlobalRole.OPERATOR, true);
        when(repository.findByUsername(user.username())).thenReturn(Optional.of(user));

        ReplicaDbUserDetails details = (ReplicaDbUserDetails) service.loadUserByUsername(user.username());

        assertEquals(user.id(), details.userId());
        assertEquals(user.passwordHash(), details.getPassword());
        assertTrue(details.isEnabled());
        assertEquals(1, details.getAuthorities().size());
        assertEquals("ROLE_OPERATOR", details.getAuthorities().iterator().next().getAuthority());
    }

    @Test
    void exposesDisabledUserAsDisabled() {
        AppUser user = user(GlobalRole.VIEWER, false);
        when(repository.findByUsername(user.username())).thenReturn(Optional.of(user));

        ReplicaDbUserDetails details = (ReplicaDbUserDetails) service.loadUserByUsername(user.username());

        assertFalse(details.isEnabled());
        assertFalse(details.isAccountNonExpired());
        assertFalse(details.isAccountNonLocked());
        assertFalse(details.isCredentialsNonExpired());
    }

    @Test
    void rejectsUnknownUsername() {
        when(repository.findByUsername("missing-user")).thenReturn(Optional.empty());

        assertThrows(UsernameNotFoundException.class, () -> service.loadUserByUsername("missing-user"));
    }

    private static AppUser user(GlobalRole role, boolean enabled) {
        return new AppUser(UUID.randomUUID(), "security-user", "password-hash", role, enabled, null, null);
    }
}
