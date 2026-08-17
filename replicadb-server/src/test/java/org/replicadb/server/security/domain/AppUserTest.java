package org.replicadb.server.security.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AppUserTest {

    @Test
    void acceptsValidUser() {
        assertDoesNotThrow(() -> new AppUser(
                UUID.randomUUID(), "admin.user", "{argon2}hash", GlobalRole.ADMIN, true, null, null));
    }

    @Test
    void rejectsBlankOrInvalidUsername() {
        assertThrows(IllegalArgumentException.class, () -> user(""));
        assertThrows(IllegalArgumentException.class, () -> user("bad username"));
        assertThrows(IllegalArgumentException.class, () -> user("ab"));
    }

    @Test
    void rejectsBlankPasswordHash() {
        assertThrows(IllegalArgumentException.class, () -> new AppUser(
                UUID.randomUUID(), "admin", " ", GlobalRole.ADMIN, true, null, null));
    }

    @Test
    void rejectsNullRole() {
        assertThrows(IllegalArgumentException.class, () -> new AppUser(
                UUID.randomUUID(), "admin", "hash", null, true, null, null));
    }

    private static AppUser user(String username) {
        return new AppUser(UUID.randomUUID(), username, "hash", GlobalRole.VIEWER, true, null, null);
    }
}
