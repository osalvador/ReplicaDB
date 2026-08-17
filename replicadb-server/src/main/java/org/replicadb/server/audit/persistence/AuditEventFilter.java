package org.replicadb.server.audit.persistence;

import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditResourceType;

import java.time.Instant;
import java.util.UUID;

public record AuditEventFilter(
        UUID actorUserId,
        AuditAction action,
        AuditResourceType resourceType,
        String resourceId,
        Instant from,
        Instant to) {

    public AuditEventFilter {
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("from must not be after to");
        }
    }

    public static AuditEventFilter empty() {
        return new AuditEventFilter(null, null, null, null, null, null);
    }
}
