package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectionCredentialsTest {

    @Test
    void rejectsBlankConnect() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionCredentials(" ", null, null, null, null));
    }

    @Test
    void rejectsEmbeddedCredentials() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionCredentials("jdbc:postgresql://user:secret@host/db", null, null, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionCredentials("jdbc:postgresql://host/db?password=secret", null, null, null, null));
    }

    @Test
    void rejectsLiteralPassword() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionCredentials("jdbc:source", null, "plain-secret", null, null));
    }

    @Test
    void acceptsEnvironmentPasswordReference() {
        ConnectionCredentials credentials = new ConnectionCredentials(
                "jdbc:source", null, "${env:SOURCE_PASSWORD}", null, null);

        assertEquals("${env:SOURCE_PASSWORD}", credentials.password());
    }

    @Test
    void defaultsAuthenticationAndConnectionParams() {
        ConnectionCredentials credentials = new ConnectionCredentials("jdbc:source", null, null, null, null);

        assertNotNull(credentials.authentication());
        assertEquals(Map.of(), credentials.connectionParams());
    }

    @Test
    void rejectsCredentialLikeConnectionParams() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionCredentials("jdbc:source", null, null, null,
                        Map.of("password", "not-a-secret")));
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionCredentials("jdbc:source", null, null, null,
                        Map.of("applicationName", "contains-secret-marker")));
    }

    @Test
    void copiesConnectionParams() {
        Map<String, String> input = new HashMap<>();
        input.put("ApplicationName", "ReplicaDB");

        ConnectionCredentials credentials = new ConnectionCredentials("jdbc:source", null, null, null, input);
        input.put("ApplicationName", "changed");

        assertEquals(Map.of("ApplicationName", "ReplicaDB"), credentials.connectionParams());
        assertThrows(UnsupportedOperationException.class,
                () -> credentials.connectionParams().put("sslmode", "require"));
    }
}
