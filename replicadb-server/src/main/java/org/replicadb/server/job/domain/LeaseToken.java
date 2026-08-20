package org.replicadb.server.job.domain;

import java.util.Objects;
import java.util.UUID;

public record LeaseToken(UUID value) {

    public LeaseToken {
        Objects.requireNonNull(value, "value must not be null");
    }

    public static LeaseToken generate() {
        return new LeaseToken(UUID.randomUUID());
    }
}
