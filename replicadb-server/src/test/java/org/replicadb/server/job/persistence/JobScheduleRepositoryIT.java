package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobSchedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobScheduleRepositoryIT {

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobScheduleRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_schedule, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void upsertInsertsAndReplacesTheSchedule() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobSchedule inserted = repository.upsert(schedule(definition.id(), "UTC", true));

        jdbcTemplate.update("UPDATE job_schedule SET updated_at = now() - interval '1 second' "
                + "WHERE job_definition_id = :jobDefinitionId",
                Map.of("jobDefinitionId", definition.id()));
        JobSchedule replacement = repository.upsert(schedule(definition.id(), "Europe/Madrid", false));

        assertEquals(definition.id(), inserted.jobDefinitionId());
        assertEquals("0 0 * * * ?", inserted.cronExpression());
        assertEquals("Europe/Madrid", replacement.timeZone());
        assertFalse(replacement.enabled());
        assertEquals(inserted.createdAt(), replacement.createdAt());
        assertTrue(replacement.updatedAt().isAfter(inserted.updatedAt()));
    }

    @Test
    void returnsEmptyForAnUnknownJobDefinition() {
        assertTrue(repository.findByJobDefinitionId(UUID.randomUUID()).isEmpty());
    }

    @Test
    void findsOnlyEnabledSchedules() {
        JobDefinition enabledDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition disabledDefinition = jobDefinitionRepository.insert(definition());
        repository.upsert(schedule(enabledDefinition.id(), "UTC", true));
        repository.upsert(schedule(disabledDefinition.id(), "UTC", false));

        assertEquals(java.util.List.of(enabledDefinition.id()), repository.findAllEnabled().stream()
                .map(JobSchedule::jobDefinitionId)
                .toList());
    }

    @Test
    void deletesAnExistingScheduleAndIsIdempotent() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        repository.upsert(schedule(definition.id(), "UTC", true));

        assertTrue(repository.delete(definition.id()));
        assertFalse(repository.delete(definition.id()));
        assertTrue(repository.findByJobDefinitionId(definition.id()).isEmpty());
    }

    private static JobDefinition definition() {
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName("schedule-job-" + UUID.randomUUID())
            .withSourcePassword("${env:SOURCE_PASSWORD}")
            .withSinkPassword("${env:SINK_PASSWORD}")
            .build();
    }

    private static JobSchedule schedule(UUID jobDefinitionId, String timeZone, boolean enabled) {
        return new JobSchedule(jobDefinitionId, "0 0 * * * ?", timeZone, enabled,
                Instant.now(), Instant.now());
    }
}
