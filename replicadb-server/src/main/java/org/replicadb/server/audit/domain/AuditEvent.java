package org.replicadb.server.audit.domain;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record AuditEvent(
        UUID id,
        Instant occurredAt,
        AuditActor actor,
        AuditAction action,
        AuditResourceType resourceType,
        String resourceId,
        AuditOutcome outcome,
        Map<String, String> detail) {

    public AuditEvent {
        Objects.requireNonNull(actor, "actor must not be null");
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(resourceType, "resourceType must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        detail = detail == null ? Map.of() : Map.copyOf(detail);
    }
}
