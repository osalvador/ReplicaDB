package org.replicadb.server.security.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobPermissionTest {

    @Test
    void acceptsValidPermission() {
        assertDoesNotThrow(() -> new JobPermission(
                UUID.randomUUID(), UUID.randomUUID(), JobPermissionType.VIEW, null));
    }

    @Test
    void rejectsNullJobDefinitionId() {
        assertThrows(IllegalArgumentException.class, () -> new JobPermission(
                null, UUID.randomUUID(), JobPermissionType.VIEW, null));
    }

    @Test
    void rejectsNullUserId() {
        assertThrows(IllegalArgumentException.class, () -> new JobPermission(
                UUID.randomUUID(), null, JobPermissionType.VIEW, null));
    }

    @Test
    void rejectsNullPermission() {
        assertThrows(IllegalArgumentException.class, () -> new JobPermission(
                UUID.randomUUID(), UUID.randomUUID(), null, null));
    }
}
