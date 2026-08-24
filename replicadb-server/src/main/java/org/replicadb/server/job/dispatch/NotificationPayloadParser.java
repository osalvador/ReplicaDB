package org.replicadb.server.job.dispatch;

import java.util.Optional;
import java.util.UUID;

public final class NotificationPayloadParser {

    public Optional<UUID> parse(String payload) {
        if (payload == null || payload.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(payload.trim()));
        } catch (IllegalArgumentException exception) {
            return Optional.empty();
        }
    }
}