package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.replicadb.server.job.api.DatasourceRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSourceSecurityKeyPolicyTest {

    @Test
    void classifiesManagerSpecificSecurityKeys() {
        assertTrue(DataSourceSecurityKeyPolicy.isSecurityKey("password"));
        assertTrue(DataSourceSecurityKeyPolicy.isSecurityKey("auth.client.key"));
        assertTrue(DataSourceSecurityKeyPolicy.isSecurityKey("connect.parameter.accessKey"));
        assertTrue(DataSourceSecurityKeyPolicy.isSecurityKey("connect.parameter.secretKey"));
        assertTrue(DataSourceSecurityKeyPolicy.isSecurityKey("connect.parameter.sasl.jaas.config"));
        assertTrue(DataSourceSecurityKeyPolicy.isSecurityKey("connect.parameter.ssl.keystore.password"));
        assertFalse(DataSourceSecurityKeyPolicy.isSecurityKey("connect.parameter.topic"));
        assertFalse(DataSourceSecurityKeyPolicy.isSecurityKey("format.delimiter"));
    }

    @Test
    void rejectsSensitiveTechnicalParameters() {
        assertThrows(IllegalArgumentException.class, () ->
                DataSourceSecurityKeyPolicy.validateTechnicalParameters(
                        Map.of("secretKey", "value")));
        assertThrows(IllegalArgumentException.class, () ->
                DataSourceSecurityKeyPolicy.validateTechnicalParameters(
                        Map.of("client.option", "password=embedded")));
    }

    @Test
    void mergesUpdatesWithoutClearingExistingSecurity() {
        Map<String, String> merged = DataSourceSecurityKeyPolicy.mergeSecurityParameters(
                Map.of("connect", "jdbc:source", "password", "old", "user", "source-user"),
                Map.of("password", "new"), Set.of());

        assertEquals("jdbc:source", merged.get("connect"));
        assertEquals("new", merged.get("password"));
        assertEquals("source-user", merged.get("user"));
    }

        @Test
        void blankRequestedSecurityPreservesExistingValue() {
                Map<String, String> merged = DataSourceSecurityKeyPolicy.mergeSecurityParameters(
                                Map.of("connect", "jdbc:source", "password", "old"),
                                Map.of("password", ""), Set.of());

                assertEquals("old", merged.get("password"));
        }

    @Test
    void requiresExplicitClearAndNeverAllowsConnectionRemoval() {
        Map<String, String> cleared = DataSourceSecurityKeyPolicy.mergeSecurityParameters(
                Map.of("connect", "jdbc:source", "password", "old", "user", "source-user"),
                Map.of(), Set.of("password"));
        assertFalse(cleared.containsKey("password"));

        assertThrows(IllegalArgumentException.class, () ->
                DataSourceSecurityKeyPolicy.mergeSecurityParameters(
                        Map.of("connect", "jdbc:source"), Map.of(), Set.of("connect")));
        assertThrows(IllegalArgumentException.class, () ->
                DataSourceSecurityKeyPolicy.mergeSecurityParameters(
                        Map.of(), Map.of("password", "value"), Set.of()));
    }

    @Test
    void datasourceRequestDefaultsMutableCollectionsToEmptyValues() {
        DatasourceRequest request = new DatasourceRequest(
                "source", "postgres", null, null, null);

        assertEquals(Map.of(), request.technicalParams());
        assertEquals(Map.of(), request.security());
        assertEquals(Set.of(), request.clearSecurityKeys());
    }
}
