package org.replicadb.server.security.domain;

import java.time.Instant;
import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Pattern;

public record AppUser(
        UUID id,
        String username,
        String passwordHash,
        GlobalRole role,
        boolean enabled,
        Instant createdAt,
        Instant updatedAt) implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern USERNAME = Pattern.compile("[A-Za-z0-9._-]{3,100}");

    public AppUser {
        if (username == null || !USERNAME.matcher(username).matches()) {
            throw new IllegalArgumentException("username must contain 3 to 100 letters, digits, '.', '_' or '-'");
        }
        if (passwordHash == null || passwordHash.isBlank()) {
            throw new IllegalArgumentException("passwordHash must not be blank");
        }
        if (role == null) {
            throw new IllegalArgumentException("role must not be null");
        }
    }
}
