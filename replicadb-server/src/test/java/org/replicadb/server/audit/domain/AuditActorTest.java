package org.replicadb.server.audit.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AuditActorTest {

    @Test
    void acceptsAValidActor() {
        UUID userId = UUID.randomUUID();

        AuditActor actor = assertDoesNotThrow(() -> new AuditActor(userId, "admin", "127.0.0.1"));

        assertEquals(userId, actor.userId());
        assertEquals("admin", actor.username());
        assertEquals("127.0.0.1", actor.sourceAddress());
    }

    @Test
    void rejectsNullUsername() {
        assertThrows(IllegalArgumentException.class,
                () -> new AuditActor(null, null, null));
    }

    @Test
    void rejectsBlankUsername() {
        assertThrows(IllegalArgumentException.class,
                () -> new AuditActor(null, "   ", null));
    }

    @Test
    void truncatesSourceAddressToDatabaseLimit() {
        String sourceAddress = "a".repeat(60);

        AuditActor actor = new AuditActor(null, "admin", sourceAddress);

        assertEquals(45, actor.sourceAddress().length());
    }

    @Test
    void createsSystemActor() {
        AuditActor actor = AuditActor.system("scheduler");

        assertNull(actor.userId());
        assertEquals("system:scheduler", actor.username());
        assertNull(actor.sourceAddress());
    }
}
