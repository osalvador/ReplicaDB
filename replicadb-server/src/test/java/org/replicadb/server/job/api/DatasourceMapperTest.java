package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.DataSourceCapabilityCatalog;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatasourceMapperTest {

    private final DatasourceMapper mapper = new DatasourceMapper();

    @Test
    void mapsSecurityToRedactedDisplayAndDoesNotExposeEnvelope() {
        UUID id = UUID.randomUUID();
        DatasourceRequest request = new DatasourceRequest(
                "orders", "postgres", Map.of("sslmode", "require"),
                Map.of("connect", "jdbc:postgresql://user:password@host/db", "password", "secret"), Set.of());
        EncryptedSecurityBundle bundle = new EncryptedSecurityBundle(
                1, "AES-256-GCM", "key-1", new byte[]{1}, new byte[]{2}, new byte[]{3});

        ManagedDataSource dataSource = mapper.toDataSource(id, request,
                request.security(), bundle, new byte[]{9, 8, 7}, null, null);
        DatasourceResponse response = mapper.toResponse(dataSource,
                new DataSourceCapabilityCatalog().forType(ConnectorType.POSTGRES),
                true, true, false);

        assertEquals("jdbc:postgresql://[REDACTED]@host/db", dataSource.safeConnectDisplay());
        assertEquals("jdbc:postgresql://[REDACTED]@host/db", response.safeConnectDisplay());
        assertEquals(Map.of("sslmode", "require"), response.technicalParams());
        assertTrue(response.securityConfigured());
        assertFalse(response.toString().contains("password@host"));
        assertFalse(response.toString().contains("key-1"));
        assertFalse(response.toString().contains("[B@"));
    }

    @Test
    void rejectsCredentialLikeTechnicalParametersBeforeMapping() {
        DatasourceRequest request = new DatasourceRequest(
                "orders", "postgres", Map.of("secretKey", "value"),
                Map.of("connect", "jdbc:postgresql://host/db"), Set.of());
        EncryptedSecurityBundle bundle = new EncryptedSecurityBundle(
                1, "AES-256-GCM", "key-1", new byte[]{1}, new byte[]{2}, new byte[]{3});

        assertThrows(IllegalArgumentException.class, () -> mapper.toDataSource(
                UUID.randomUUID(), request, request.security(), bundle, new byte[]{1}, null, null));
    }
}
