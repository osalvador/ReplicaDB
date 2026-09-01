package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ConnectionCredentials;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobDefinitionRepositoryIT {

    private static final UUID SOURCE_DATASOURCE_ID = UUID.randomUUID();
    private static final UUID SINK_DATASOURCE_ID = UUID.randomUUID();

    @Autowired
    private JobDefinitionRepository repository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(dataSource(SOURCE_DATASOURCE_ID, "source-profile", ConnectorType.POSTGRES));
        managedDataSourceRepository.insert(dataSource(SINK_DATASOURCE_ID, "sink-profile", ConnectorType.POSTGRES));
    }

    @Test
    void insertsAndReadsDefinitionById() {
        JobDefinition inserted = repository.insert(definition("round-trip-" + UUID.randomUUID()));

        JobDefinition found = repository.findById(inserted.id()).orElseThrow();

        assertEquals(inserted, found);
        assertNotNull(found.createdAt());
        assertNotNull(found.updatedAt());
        assertTrue(found.source().connection() == null);
        assertTrue(found.sink().connection() == null);
        assertEquals(3, found.maxAttempts());
        assertEquals(60, found.retryBackoffSeconds());
        assertFalse(found.automaticRetryEnabled());
    }

    @Test
    void returnsEmptyForUnknownName() {
        assertTrue(repository.findByName("missing-" + UUID.randomUUID()).isEmpty());
    }

    @Test
    void rejectsDuplicateNames() {
        JobDefinition definition = definition("duplicate-" + UUID.randomUUID());
        repository.insert(definition);

        assertThrows(DataIntegrityViolationException.class, () -> repository.insert(definition));
    }

    @Test
    void findsDefinitionsByNameAndListsPersistedRows() {
        JobDefinition inserted = repository.insert(definition("find-name-" + UUID.randomUUID()));

        assertEquals(inserted, repository.findByName(inserted.name()).orElseThrow());
        assertFalse(repository.findAll().isEmpty());
    }

        @Test
        void paginatesDefinitionsInNameOrder() {
        repository.insert(definition("page-c"));
        repository.insert(definition("page-a"));
        repository.insert(definition("page-e"));
        repository.insert(definition("page-b"));
        repository.insert(definition("page-d"));

        assertEquals(5, repository.count(null));
        assertEquals(java.util.List.of("page-a", "page-b"), repository.findPage(0, 2, null)
            .stream().map(JobDefinition::name).toList());
        assertEquals(java.util.List.of("page-c", "page-d"), repository.findPage(1, 2, null)
            .stream().map(JobDefinition::name).toList());
        assertEquals(java.util.List.of("page-e"), repository.findPage(2, 2, null)
            .stream().map(JobDefinition::name).toList());
        }

        @Test
        void restrictsPagesAndCountsToAllowedIds() {
        JobDefinition first = repository.insert(definition("restricted-a"));
        JobDefinition second = repository.insert(definition("restricted-b"));
        repository.insert(definition("restricted-c"));

        assertEquals(2, repository.count(Set.of(first.id(), second.id())));
        assertEquals(java.util.List.of("restricted-a", "restricted-b"),
            repository.findPage(0, 10, Set.of(first.id(), second.id()))
                .stream().map(JobDefinition::name).toList());
        assertEquals(0, repository.count(Set.of()));
        assertTrue(repository.findPage(0, 10, Set.of()).isEmpty());
        }

        @Test
        void updatesAllMutableFieldsAndRefreshesUpdatedAt() {
        JobDefinition inserted = repository.insert(definition("update-source"));
        jdbcTemplate.update("UPDATE job_definition SET updated_at = now() - interval '1 second' WHERE id = :id",
            Map.of("id", inserted.id()));
        JobDefinition stale = repository.findById(inserted.id()).orElseThrow();
        JobDefinition replacement = JobDefinitionTestFixtures.aJobDefinition()
            .withId(inserted.id())
            .withName(inserted.name())
            .withSourceDatasourceId(SOURCE_DATASOURCE_ID)
            .withSourceTable("updated_source_table")
            .withSourceWhere("id > 10")
            .withSinkDatasourceId(SINK_DATASOURCE_ID)
            .withSinkTable("updated_sink_table")
            .withMode(ReplicationMode.INCREMENTAL)
            .withJobs(4)
            .withIncrementalWatermarkColumn("updated_at")
            .withInitialWatermarkValue("100")
            .withRetryPolicy(new RetryPolicy(7, 90, true))
            .withCreatedAt(stale.createdAt())
            .withUpdatedAt(stale.updatedAt())
            .build();

        JobDefinition updated = repository.update(replacement);

        assertEquals(replacement.sourceDatasourceId(), updated.sourceDatasourceId());
        assertEquals(replacement.sourceTable(), updated.sourceTable());
        assertEquals(replacement.sourceWhere(), updated.sourceWhere());
        assertEquals(replacement.sinkDatasourceId(), updated.sinkDatasourceId());
        assertEquals(replacement.sinkTable(), updated.sinkTable());
        assertEquals(replacement.mode(), updated.mode());
        assertEquals(replacement.jobs(), updated.jobs());
        assertEquals(replacement.incrementalWatermarkColumn(), updated.incrementalWatermarkColumn());
        assertEquals(replacement.initialWatermarkValue(), updated.initialWatermarkValue());
        assertTrue(updated.updatedAt().isAfter(stale.updatedAt()));
        assertEquals(inserted.name(), updated.name());
        assertEquals(inserted.createdAt(), updated.createdAt());
        assertEquals(7, updated.maxAttempts());
        assertEquals(90, updated.retryBackoffSeconds());
        assertTrue(updated.automaticRetryEnabled());
        }

        @Test
        void rejectsUpdateForUnknownDefinition() {
            JobDefinition template = definition("unknown-update");
            JobDefinition unknown = JobDefinitionTestFixtures.aJobDefinition()
                .withId(UUID.randomUUID())
                .withName(template.name())
                .withSourceDatasourceId(template.sourceDatasourceId())
                .withSourceTable(template.sourceTable())
                .withSourceWhere(template.sourceWhere())
                .withSinkDatasourceId(template.sinkDatasourceId())
                .withSinkTable(template.sinkTable())
                .withMode(template.mode())
                .withJobs(template.jobs())
                .withIncrementalWatermarkColumn(template.incrementalWatermarkColumn())
                .withInitialWatermarkValue(template.initialWatermarkValue())
                .withCreatedAt(Instant.now())
                .withUpdatedAt(Instant.now())
                .build();

        assertThrows(NoSuchElementException.class, () -> repository.update(unknown));
        }

        @Test
        void roundTripsAdvancedOptions() {
        JobDefinition original = new JobDefinition(
            null, "advanced-" + UUID.randomUUID(),
            new SourceEndpoint(
                SOURCE_DATASOURCE_ID,
                null, "id, name", "id > 10", "select id, name from source_table"),
            new SinkEndpoint(
                SINK_DATASOURCE_ID,
                "sink_table", "id, name", new org.replicadb.server.job.domain.StagingOptions(
                    "staging", "sink_stage"), true, true),
            true, true, ReplicationMode.INCREMENTAL, 3, "updated_at", "0", null, null, 250, 512, true,
            new RetryPolicy(3, 60, true));

        JobDefinition inserted = repository.insert(original);
        JobDefinition found = repository.findById(inserted.id()).orElseThrow();

        assertEquals(inserted, found);
        assertEquals("select id, name from source_table", found.sourceQuery());
        assertEquals(SOURCE_DATASOURCE_ID, found.sourceDatasourceId());
        assertEquals(SINK_DATASOURCE_ID, found.sinkDatasourceId());
        assertEquals("staging", found.sinkStagingSchema());
        assertEquals("sink_stage", found.sinkStagingTable());
        assertTrue(found.sinkDisableEscape());
        assertTrue(found.sinkDisableTruncate());
        assertEquals(250, found.fetchSize());
        assertEquals(512, found.bandwidthThrottling());
        assertTrue(found.verbose());
        }

    private static JobDefinition definition(String name) {
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName(name)
            .withSourceDatasourceId(SOURCE_DATASOURCE_ID)
            .withSinkDatasourceId(SINK_DATASOURCE_ID)
            .withJobs(2)
            .build();
    }

    private static ManagedDataSource dataSource(UUID id, String name, ConnectorType connectorType) {
        return new ManagedDataSource(id, name, connectorType,
                "jdbc:postgresql://host/db", Map.of("sslmode", "require"), new byte[]{1, 2, 3},
                1, "AES-256-GCM", "test", null, null);
    }
}
