package org.replicadb.server.security.auth;

import java.util.Objects;
import java.util.UUID;

public record LoginAttemptReservation(UUID id, String usernameKey, String addressKey) {

    public LoginAttemptReservation {
        Objects.requireNonNull(id, "id must not be null");
        requireNonBlank("usernameKey", usernameKey);
        requireNonBlank("addressKey", addressKey);
    }

    @Override
    public String toString() {
        return "LoginAttemptReservation[id=" + id + "]";
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
