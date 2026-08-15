package org.replicadb.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CoreDependencyResolutionTest {

    @Test
    void resolvesPublicReplicaDbClassFromCoreArtifact() {
        Class<?> replicaDbClass = assertDoesNotThrow(
                () -> Class.forName("org.replicadb.ReplicaDB"));

        assertEquals("org.replicadb", replicaDbClass.getPackage().getName());
        assertTrue(java.lang.reflect.Modifier.isPublic(replicaDbClass.getModifiers()));
    }
}