package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.JobDefinition;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobDefinitionMapperTest {

    private final JobDefinitionMapper mapper = new JobDefinitionMapper();

    @Test
    void mapsRequestToDefinitionAndResponseWithoutSecrets() {
        JobDefinitionRequest request = request("complete");
        JobDefinition definition = mapper.toDefinition(request, UUID.randomUUID(), request.name(),
                Instant.now(), Instant.now());
        JobDefinitionResponse response = mapper.toResponse(definition);

        assertEquals(request.sourceConnect(), definition.sourceConnect());
        assertEquals(request.sourceWhere(), definition.sourceWhere());
        assertEquals(request.mode(), response.mode());
        assertEquals(request.jobs(), response.jobs());
        assertTrue(response.sourcePasswordConfigured());
        assertTrue(response.sinkPasswordConfigured());
        assertNotNull(response.modeWarning());
        assertFalse(response.toString().contains("SOURCE_PASSWORD"));
        assertFalse(response.toString().contains("SINK_PASSWORD"));
    }

    @Test
    void onlyCompleteModeHasWarning() {
        JobDefinitionResponse incremental = mapper.toResponse(mapper.toDefinition(
            request("incremental"), UUID.randomUUID(), "incremental", null, null));
        JobDefinitionResponse atomic = mapper.toResponse(mapper.toDefinition(
            request("complete-atomic"), UUID.randomUUID(), "atomic", null, null));

        assertNull(incremental.modeWarning());
        assertNull(atomic.modeWarning());
    }

    @Test
    void preservesSourcePasswordWhenSinkPasswordIsReplaced() {
        JobDefinition definition = mapper.toDefinition(
                requestWithPasswords("", "${env:NEW_SINK_PASSWORD}"), UUID.randomUUID(), "job-name",
                null, null, "${env:EXISTING_SOURCE_PASSWORD}", "${env:EXISTING_SINK_PASSWORD}");

        assertEquals("${env:EXISTING_SOURCE_PASSWORD}", definition.sourcePassword());
        assertEquals("${env:NEW_SINK_PASSWORD}", definition.sinkPassword());
    }

    @Test
    void preservesSinkPasswordWhenSourcePasswordIsReplaced() {
        JobDefinition definition = mapper.toDefinition(
                requestWithPasswords("${env:NEW_SOURCE_PASSWORD}", ""), UUID.randomUUID(), "job-name",
                null, null, "${env:EXISTING_SOURCE_PASSWORD}", "${env:EXISTING_SINK_PASSWORD}");

        assertEquals("${env:NEW_SOURCE_PASSWORD}", definition.sourcePassword());
        assertEquals("${env:EXISTING_SINK_PASSWORD}", definition.sinkPassword());
    }

    @Test
    void preservesBothPasswordsWhenUpdateValuesAreBlank() {
        JobDefinition definition = mapper.toDefinition(
                requestWithPasswords("", ""), UUID.randomUUID(), "job-name", null, null,
                "${env:EXISTING_SOURCE_PASSWORD}", "${env:EXISTING_SINK_PASSWORD}");

        assertEquals("${env:EXISTING_SOURCE_PASSWORD}", definition.sourcePassword());
        assertEquals("${env:EXISTING_SINK_PASSWORD}", definition.sinkPassword());
    }

    @Test
    void createPathStillAcceptsNullPasswords() {
        JobDefinition definition = mapper.toDefinition(
                requestWithPasswords(null, null), UUID.randomUUID(), "job-name", null, null);

        assertNull(definition.sourcePassword());
        assertNull(definition.sinkPassword());
    }

    @Test
    void createPathStillRejectsBlankPasswords() {
        assertThrows(IllegalArgumentException.class, () -> mapper.toDefinition(
                requestWithPasswords("", ""), UUID.randomUUID(), "job-name", null, null));
    }

    @Test
    void mapsAdvancedFieldsAndRedactsConnectionParams() {
        JobDefinition definition = mapper.toDefinition(advancedRequest(), UUID.randomUUID(), "advanced", null, null);
        JobDefinitionResponse response = mapper.toResponse(definition);

        assertNull(response.sourceTable());
        assertEquals("select id, name from source_table", response.sourceQuery());
        assertEquals("id, name", response.sourceColumns());
        assertEquals("ActiveDirectoryDefault", response.sourceAuthMode());
        assertEquals("[REDACTED]", response.sourceConnectionParams().get("clientId"));
        assertEquals("staging", response.sinkStagingSchema());
        assertEquals("sink_stage", response.sinkStagingTable());
        assertTrue(response.sinkDisableEscape());
        assertTrue(response.sinkDisableTruncate());
        assertEquals(250, response.fetchSize());
        assertEquals(512, response.bandwidthThrottling());
        assertTrue(response.verbose());
    }

    private static JobDefinitionRequest request(String mode) {
        return new JobDefinitionRequest(
                "job-name", "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", "source_table", "id > 0",
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table", mode, 2,
                "incremental".equals(mode) ? "updated_at" : null,
                "incremental".equals(mode) ? "0" : null);
    }

    private static JobDefinitionRequest requestWithPasswords(String sourcePassword, String sinkPassword) {
        return new JobDefinitionRequest(
                "job-name", "jdbc:source", "source-user", sourcePassword, "source_table", null,
                "jdbc:sink", "sink-user", sinkPassword, "sink_table", "complete", 1, null, null);
    }

    private static JobDefinitionRequest advancedRequest() {
        return new JobDefinitionRequest(
                "advanced", "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", null, "id > 0",
                "ActiveDirectoryDefault", "source-client", "source-login", "source-cert", "source-key",
                Map.of("clientId", "source-client"), "id, name", "select id, name from source_table",
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table",
                "ActiveDirectoryManagedIdentity", "sink-client", null, null, null,
                Map.of("ApplicationName", "ReplicaDB"), "id, name", "staging", "sink_stage", true, true,
                "incremental", 3, "updated_at", "0", 250, 512, true);
    }
}
