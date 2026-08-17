package org.replicadb.server.audit.domain;

import java.util.UUID;

public record AuditActor(UUID userId, String username, String sourceAddress) {

    public AuditActor {
        if (username == null) {
            throw new IllegalArgumentException("username must not be null");
        }
        if (username.isBlank()) {
            throw new IllegalArgumentException("username must not be blank");
        }
        if (sourceAddress != null && sourceAddress.length() > 45) {
            sourceAddress = sourceAddress.substring(0, 45);
        }
    }

    public static AuditActor system(String identity) {
        return new AuditActor(null, "system:" + identity, null);
    }
}
