package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
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

    @Autowired
    private JobDefinitionRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void insertsAndReadsDefinitionById() {
        JobDefinition inserted = repository.insert(definition("round-trip-" + UUID.randomUUID()));

        JobDefinition found = repository.findById(inserted.id()).orElseThrow();

        assertEquals(inserted, found);
        assertNotNull(found.createdAt());
        assertNotNull(found.updatedAt());
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
        JobDefinition replacement = new JobDefinition(
            inserted.id(), inserted.name(), "jdbc:updated-source", "updated-source-user",
            "${env:UPDATED_SOURCE_PASSWORD}", "updated_source_table", "id > 10",
            "jdbc:updated-sink", "updated-sink-user", "${env:UPDATED_SINK_PASSWORD}",
            "updated_sink_table", ReplicationMode.INCREMENTAL, 4, "updated_at", "100",
            stale.createdAt(), stale.updatedAt());

        JobDefinition updated = repository.update(replacement);

        assertEquals(replacement.sourceConnect(), updated.sourceConnect());
        assertEquals(replacement.sourceUser(), updated.sourceUser());
        assertEquals(replacement.sourcePassword(), updated.sourcePassword());
        assertEquals(replacement.sourceTable(), updated.sourceTable());
        assertEquals(replacement.sourceWhere(), updated.sourceWhere());
        assertEquals(replacement.sinkConnect(), updated.sinkConnect());
        assertEquals(replacement.sinkUser(), updated.sinkUser());
        assertEquals(replacement.sinkPassword(), updated.sinkPassword());
        assertEquals(replacement.sinkTable(), updated.sinkTable());
        assertEquals(replacement.mode(), updated.mode());
        assertEquals(replacement.jobs(), updated.jobs());
        assertEquals(replacement.incrementalWatermarkColumn(), updated.incrementalWatermarkColumn());
        assertEquals(replacement.initialWatermarkValue(), updated.initialWatermarkValue());
        assertTrue(updated.updatedAt().isAfter(stale.updatedAt()));
        assertEquals(inserted.name(), updated.name());
        assertEquals(inserted.createdAt(), updated.createdAt());
        }

        @Test
        void rejectsUpdateForUnknownDefinition() {
            JobDefinition template = definition("unknown-update");
            JobDefinition unknown = new JobDefinition(UUID.randomUUID(), template.name(), template.sourceConnect(),
                template.sourceUser(), template.sourcePassword(), template.sourceTable(), template.sourceWhere(),
                template.sinkConnect(), template.sinkUser(), template.sinkPassword(), template.sinkTable(),
                template.mode(), template.jobs(), template.incrementalWatermarkColumn(),
                template.initialWatermarkValue(), Instant.now(), Instant.now());

        assertThrows(NoSuchElementException.class, () -> repository.update(unknown));
        }

    private static JobDefinition definition(String name) {
        return new JobDefinition(
                null, name, "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", "source_table", null,
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table", ReplicationMode.COMPLETE,
                2, null, null, null, null);
    }
}
