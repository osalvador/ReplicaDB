package org.replicadb.server.audit.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditEventTest {

    @Test
    void acceptsAFullyPopulatedEvent() {
        UUID id = UUID.randomUUID();
        Instant occurredAt = Instant.now();
        AuditActor actor = new AuditActor(UUID.randomUUID(), "admin", "127.0.0.1");
        Map<String, String> detail = Map.of("reason", "manual");

        AuditEvent event = new AuditEvent(id, occurredAt, actor, AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS, detail);

        assertEquals(id, event.id());
        assertEquals(occurredAt, event.occurredAt());
        assertEquals(actor, event.actor());
        assertEquals(AuditAction.JOB_CREATED, event.action());
        assertEquals(AuditResourceType.JOB_DEFINITION, event.resourceType());
        assertEquals("job-1", event.resourceId());
        assertEquals(AuditOutcome.SUCCESS, event.outcome());
        assertEquals(detail, event.detail());
    }

    @Test
    void rejectsNullActor() {
        assertThrows(NullPointerException.class, () -> new AuditEvent(
                null, null, null, AuditAction.JOB_CREATED, AuditResourceType.JOB_DEFINITION,
                null, AuditOutcome.SUCCESS, null));
    }

    @Test
    void rejectsNullAction() {
        assertThrows(NullPointerException.class, () -> new AuditEvent(
                null, null, AuditActor.system("api"), null, AuditResourceType.JOB_DEFINITION,
                null, AuditOutcome.SUCCESS, null));
    }

    @Test
    void rejectsNullResourceType() {
        assertThrows(NullPointerException.class, () -> new AuditEvent(
                null, null, AuditActor.system("api"), AuditAction.JOB_CREATED, null,
                null, AuditOutcome.SUCCESS, null));
    }

    @Test
    void rejectsNullOutcome() {
        assertThrows(NullPointerException.class, () -> new AuditEvent(
                null, null, AuditActor.system("api"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, null, null, null));
    }

    @Test
    void normalizesNullDetailToEmpty() {
        AuditEvent event = new AuditEvent(null, null, AuditActor.system("api"),
                AuditAction.JOB_CREATED, AuditResourceType.JOB_DEFINITION, null,
                AuditOutcome.SUCCESS, null);

        assertEquals(Map.of(), event.detail());
    }

    @Test
    void defensivelyCopiesDetail() {
        Map<String, String> detail = new HashMap<>();
        detail.put("key", "value");

        AuditEvent event = new AuditEvent(null, null, AuditActor.system("api"),
                AuditAction.JOB_CREATED, AuditResourceType.JOB_DEFINITION, null,
                AuditOutcome.SUCCESS, detail);
        detail.put("other", "changed");

        assertEquals(Map.of("key", "value"), event.detail());
        assertNotNull(event.detail());
    }

    @Test
    void enumNamesFitDatabaseColumns() {
        for (AuditAction action : AuditAction.values()) {
            assertTrue(action.name().length() <= 60, action.name());
        }
        for (AuditResourceType resourceType : AuditResourceType.values()) {
            assertTrue(resourceType.name().length() <= 30, resourceType.name());
        }
    }
}
