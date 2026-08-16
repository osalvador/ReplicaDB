package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.JobDefinition;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    private static JobDefinitionRequest request(String mode) {
        return new JobDefinitionRequest(
                "job-name", "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", "source_table", "id > 0",
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table", mode, 2,
                "incremental".equals(mode) ? "updated_at" : null,
                "incremental".equals(mode) ? "0" : null);
    }
}
