package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ManagedDataSourceTest {

    @Test
    void copiesTechnicalParametersAndEncryptedBytes() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("topic", "orders");
        byte[] encrypted = {1, 2, 3};
        ManagedDataSource dataSource = new ManagedDataSource(UUID.randomUUID(), "orders-db",
                ConnectorType.POSTGRES, "jdbc:postgresql://host/db", parameters, encrypted,
                1, "AES-256-GCM", "v1", null, null);

        parameters.put("topic", "changed");
        encrypted[0] = 9;

        assertEquals("orders", dataSource.technicalParams().get("topic"));
        assertArrayEquals(new byte[]{1, 2, 3}, dataSource.encryptedSecurity());
    }

    @Test
    void rejectsInvalidIdentityAndEnvelopeMetadata() {
        assertThrows(NullPointerException.class, () -> new ManagedDataSource(null, "name",
                ConnectorType.POSTGRES, "jdbc:postgresql://host/db", Map.of(), new byte[0],
                1, "AES-256-GCM", "v1", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ManagedDataSource(UUID.randomUUID(), " ",
                ConnectorType.POSTGRES, "jdbc:postgresql://host/db", Map.of(), new byte[0],
                1, "AES-256-GCM", "v1", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ManagedDataSource(UUID.randomUUID(), "name",
                ConnectorType.POSTGRES, "jdbc:postgresql://host/db", Map.of(), new byte[0],
                0, "AES-256-GCM", "v1", null, null));
    }
}
