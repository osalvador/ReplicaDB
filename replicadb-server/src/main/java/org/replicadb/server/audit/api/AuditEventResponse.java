package org.replicadb.server.audit.api;

import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Schema(description = "Durable audit record for an authenticated or system action.")
public record AuditEventResponse(
    @Schema(description = "Audit event identifier.", format = "uuid") UUID id,
    @Schema(description = "Event timestamp in UTC.", format = "date-time") Instant occurredAt,
    @Schema(description = "Actor user identifier, or null for a system actor.", format = "uuid", nullable = true) UUID actorUserId,
    @Schema(description = "Username snapshot or stable system actor name.") String actorUsername,
    @Schema(description = "Recorded source address when available.", nullable = true) String sourceAddress,
    @Schema(description = "Audited action category.") AuditAction action,
    @Schema(description = "Type of resource affected by the action.") AuditResourceType resourceType,
    @Schema(description = "Resource identifier as recorded by the owning operation.") String resourceId,
    @Schema(description = "Recorded action outcome.") AuditOutcome outcome,
    @Schema(description = "Bounded, non-secret action context. Keys vary by action.") Map<String, String> detail) {

    public static AuditEventResponse from(AuditEvent event) {
        return new AuditEventResponse(
                event.id(), event.occurredAt(), event.actor().userId(), event.actor().username(),
                event.actor().sourceAddress(), event.action(), event.resourceType(), event.resourceId(),
                event.outcome(), event.detail());
    }
}
