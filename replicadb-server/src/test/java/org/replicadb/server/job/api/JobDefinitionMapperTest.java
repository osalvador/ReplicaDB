package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.domain.RetryPolicy;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobDefinitionMapperTest {

    private static final UUID SOURCE_DATASOURCE_ID = UUID.fromString(
            "00000000-0000-0000-0000-000000000101");
    private static final UUID SINK_DATASOURCE_ID = UUID.fromString(
            "00000000-0000-0000-0000-000000000102");

    private final JobDefinitionMapper mapper = new JobDefinitionMapper();

    @Test
    void mapsDatasourceReferencesAndReplicationFieldsWithoutSecrets() {
        JobDefinitionRequest request = request("complete");
        JobDefinition definition = mapper.toDefinition(request, UUID.randomUUID(), request.name(),
                Instant.now(), Instant.now());
        JobDefinitionResponse response = mapper.toResponse(definition);

        assertEquals(SOURCE_DATASOURCE_ID, definition.sourceDatasourceId());
        assertEquals(SINK_DATASOURCE_ID, definition.sinkDatasourceId());
        assertTrue(definition.sourceDatasourceUseEnabled());
        assertTrue(definition.sinkDatasourceUseEnabled());
        assertEquals(request.mode(), response.mode());
        assertEquals(request.jobs(), response.jobs());
        assertEquals(SOURCE_DATASOURCE_ID, response.sourceDatasourceId());
        assertEquals(SINK_DATASOURCE_ID, response.sinkDatasourceId());
        assertNull(response.sourceDatasource());
        assertEquals(3, definition.maxAttempts());
        assertEquals(60, definition.retryBackoffSeconds());
        assertFalse(definition.automaticRetryEnabled());
        assertFalse(response.toString().contains("password"));
        assertFalse(response.toString().contains("sourceConnect"));
    }

    @Test
    void mapsSafeDatasourceSummariesWithoutEnvelopeMetadata() {
        JobDefinition definition = mapper.toDefinition(request("complete"), UUID.randomUUID(), "job-name",
                null, null);
        ManagedDataSourceSummary source = summary(SOURCE_DATASOURCE_ID, "source");
        ManagedDataSourceSummary sink = summary(SINK_DATASOURCE_ID, "sink");

        JobDefinitionResponse response = mapper.toResponse(definition, source, sink);

        assertEquals("source", response.sourceDatasource().name());
        assertEquals("postgres", response.sourceDatasource().connectorType());
        assertEquals("jdbc:postgresql://[REDACTED]@host/db",
                response.sourceDatasource().safeConnectDisplay());
        assertEquals("sink", response.sinkDatasource().name());
        assertFalse(response.toString().contains("key-1"));
        assertFalse(response.toString().contains("encryptedSecurity"));
    }

    @Test
    void preservesDisabledBindingFlagsWhenUpdateOmitsThem() {
        JobDefinition existing = mapper.toDefinition(requestWithFlags(false, false), UUID.randomUUID(),
                "job-name", null, null);
        JobDefinitionRequest update = request("complete");

        JobDefinition replacement = mapper.toDefinition(update, existing.id(), existing.name(), null, null,
                existing.retryPolicy(), existing.mode(), existing.sourceDatasourceUseEnabled(),
                existing.sinkDatasourceUseEnabled());

        assertFalse(replacement.sourceDatasourceUseEnabled());
        assertFalse(replacement.sinkDatasourceUseEnabled());
    }

    @Test
    void explicitBindingFlagsOverrideExistingValues() {
        JobDefinitionRequest request = requestWithFlags(true, false);

        JobDefinition definition = mapper.toDefinition(request, UUID.randomUUID(), "job-name", null, null,
                new RetryPolicy(3, 60, false), ReplicationMode.COMPLETE, false, true);

        assertTrue(definition.sourceDatasourceUseEnabled());
        assertFalse(definition.sinkDatasourceUseEnabled());
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
    void rejectsBlankWatermarkColumnForIncrementalMode() {
        JobDefinitionRequest request = request("incremental");
        JobDefinitionRequest invalid = new JobDefinitionRequest(
                request.name(), request.sourceDatasourceId(), request.sourceDatasourceUseEnabled(),
                request.sourceTable(), request.sourceWhere(), request.sourceColumns(), request.sourceQuery(),
                request.sinkDatasourceId(), request.sinkDatasourceUseEnabled(), request.sinkTable(),
                request.sinkColumns(), request.sinkStagingSchema(), request.sinkStagingTable(),
                request.sinkDisableEscape(), request.sinkDisableTruncate(), request.mode(), request.jobs(),
                "  ", request.initialWatermarkValue(), request.fetchSize(), request.bandwidthThrottling(),
                request.verbose(), request.maxAttempts(), request.retryBackoffSeconds(), request.automaticRetryEnabled());

        assertThrows(IllegalArgumentException.class,
                () -> mapper.toDefinition(invalid, UUID.randomUUID(), invalid.name(), null, null));
    }

    @Test
    void mapsAdvancedReplicationFields() {
        JobDefinition definition = mapper.toDefinition(advancedRequest(), UUID.randomUUID(), "advanced", null, null);
        JobDefinitionResponse response = mapper.toResponse(definition);

        assertNull(response.sourceTable());
        assertEquals("select id, name from source_table", response.sourceQuery());
        assertEquals("id, name", response.sourceColumns());
        assertEquals("staging", response.sinkStagingSchema());
        assertEquals("sink_stage", response.sinkStagingTable());
        assertTrue(response.sinkDisableEscape());
        assertTrue(response.sinkDisableTruncate());
        assertEquals(250, response.fetchSize());
        assertEquals(512, response.bandwidthThrottling());
        assertTrue(response.verbose());
    }

    @Test
    void mapsExplicitRetryPolicyFieldsToDefinitionAndResponse() {
        JobDefinitionRequest request = requestWithRetryPolicy(5, 90L, true);
        JobDefinition definition = mapper.toDefinition(request, UUID.randomUUID(), request.name(), null, null);
        JobDefinitionResponse response = mapper.toResponse(definition);

        assertEquals(5, definition.maxAttempts());
        assertEquals(90, definition.retryBackoffSeconds());
        assertTrue(definition.automaticRetryEnabled());
        assertEquals(5, response.maxAttempts());
        assertEquals(90, response.retryBackoffSeconds());
        assertTrue(response.automaticRetryEnabled());
        assertFalse(response.toString().contains("leaseToken"));
    }

    private static JobDefinitionRequest request(String mode) {
        return new JobDefinitionRequest(
                "job-name", SOURCE_DATASOURCE_ID, null, "source_table", "id > 0", null, null,
                SINK_DATASOURCE_ID, null, "sink_table", null, null, null, null, null, mode, 2,
                "incremental".equals(mode) ? "updated_at" : null,
                "incremental".equals(mode) ? "0" : null, null, null, null, null, null, null);
    }

    private static JobDefinitionRequest requestWithFlags(boolean sourceEnabled, boolean sinkEnabled) {
        JobDefinitionRequest request = request("complete");
        return new JobDefinitionRequest(request.name(), request.sourceDatasourceId(), sourceEnabled,
                request.sourceTable(), request.sourceWhere(), request.sourceColumns(), request.sourceQuery(),
                request.sinkDatasourceId(), sinkEnabled, request.sinkTable(), request.sinkColumns(),
                request.sinkStagingSchema(), request.sinkStagingTable(), request.sinkDisableEscape(),
                request.sinkDisableTruncate(), request.mode(), request.jobs(), request.incrementalWatermarkColumn(),
                request.initialWatermarkValue(), request.fetchSize(), request.bandwidthThrottling(), request.verbose(),
                request.maxAttempts(), request.retryBackoffSeconds(), request.automaticRetryEnabled());
    }

    private static JobDefinitionRequest advancedRequest() {
        return new JobDefinitionRequest(
                "advanced", SOURCE_DATASOURCE_ID, null, null, "id > 0", "id, name",
                "select id, name from source_table", SINK_DATASOURCE_ID, null, "sink_table", "id, name",
                "staging", "sink_stage", true, true, "incremental", 3, "updated_at", "0", 250, 512,
                true, null, null, null);
    }

    private static JobDefinitionRequest requestWithRetryPolicy(int maxAttempts,
                                                               Long retryBackoffSeconds,
                                                               boolean automaticRetryEnabled) {
        JobDefinitionRequest request = request("complete");
        return new JobDefinitionRequest(
                "retry-job", request.sourceDatasourceId(), null, request.sourceTable(), request.sourceWhere(),
                request.sourceColumns(), request.sourceQuery(), request.sinkDatasourceId(), null,
                request.sinkTable(), request.sinkColumns(), request.sinkStagingSchema(), request.sinkStagingTable(),
                request.sinkDisableEscape(), request.sinkDisableTruncate(), request.mode(), 1, null, null, null,
                null, null, maxAttempts, retryBackoffSeconds, automaticRetryEnabled);
    }

    private static ManagedDataSourceSummary summary(UUID id, String name) {
        return new ManagedDataSourceSummary(id, name, ConnectorType.POSTGRES,
                "jdbc:postgresql://user:password@host/db", java.util.Map.of(), true,
                1, "AES-256-GCM", "key-1", null, null);
    }
}
