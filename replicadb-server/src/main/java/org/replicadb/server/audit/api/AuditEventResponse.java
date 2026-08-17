package org.replicadb.server.audit.api;

import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record AuditEventResponse(
        UUID id,
        Instant occurredAt,
        UUID actorUserId,
        String actorUsername,
        String sourceAddress,
        AuditAction action,
        AuditResourceType resourceType,
        String resourceId,
        AuditOutcome outcome,
        Map<String, String> detail) {

    public static AuditEventResponse from(AuditEvent event) {
        return new AuditEventResponse(
                event.id(), event.occurredAt(), event.actor().userId(), event.actor().username(),
                event.actor().sourceAddress(), event.action(), event.resourceType(), event.resourceId(),
                event.outcome(), event.detail());
    }
}
