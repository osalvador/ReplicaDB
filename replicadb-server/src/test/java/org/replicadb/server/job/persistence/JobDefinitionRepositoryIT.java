package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;

import java.util.UUID;

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

    private static JobDefinition definition(String name) {
        return new JobDefinition(
                null, name, "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", "source_table", null,
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table", ReplicationMode.COMPLETE,
                2, null, null, null, null);
    }
}