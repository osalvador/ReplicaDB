package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class RunTriggerIdempotencyRepositoryIT {

    @Autowired
    private RunTriggerIdempotencyRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private JobDefinitionRepository definitions;

    @Autowired
    private ManagedDataSourceRepository dataSources;

    private UUID jobDefinitionId;

    @BeforeEach
    void setUp() {
        jdbcTemplate.update("TRUNCATE TABLE run_trigger_idempotency", Map.of());
        var source = ManagedDataSourceTestFixtures.source();
        var sink = ManagedDataSourceTestFixtures.sink();
        dataSources.findById(source.id()).orElseGet(() -> dataSources.insert(source));
        dataSources.findById(sink.id()).orElseGet(() -> dataSources.insert(sink));
        jobDefinitionId = definitions.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withDefaultDatasourceReferences()
                .build()).id();
    }

    @AfterEach
    void deleteJob() {
        if (jobDefinitionId != null) {
            definitions.delete(jobDefinitionId);
        }
    }

    @Test
    void roundTripsARecentIdempotencyKey() {
        UUID runId = UUID.randomUUID();
        repository.upsert("recent-key", jobDefinitionId, runId);

        assertEquals(runId, repository.findValidRunId("recent-key").orElseThrow());
        assertTrue(repository.findValidRunId("missing-key").isEmpty());
    }

    @Test
    void expiresKeysAtTheTwentyFourHourBoundary() {
        Instant reference = Instant.now();
        UUID insideRun = UUID.randomUUID();
        UUID outsideRun = UUID.randomUUID();
        insertRow("inside-key", insideRun, reference.minus(Duration.ofHours(24)).plusSeconds(30));
        insertRow("outside-key", outsideRun, reference.minus(Duration.ofHours(24)).minusSeconds(30));

        assertEquals(insideRun, repository.findValidRunId("inside-key").orElseThrow());
        assertTrue(repository.findValidRunId("outside-key").isEmpty());
    }

    @Test
    void upsertReplacesTheRunForAnExistingKey() {
        UUID firstRun = UUID.randomUUID();
        UUID secondRun = UUID.randomUUID();
        repository.upsert("replace-key", jobDefinitionId, firstRun);
        repository.upsert("replace-key", jobDefinitionId, secondRun);

        assertEquals(secondRun, repository.findValidRunId("replace-key").orElseThrow());
        assertEquals(1, countRows("replace-key"));
    }

    @Test
    void concurrentUpsertsLeaveOneRowForTheKey() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> first = executor.submit(() -> upsertAfter(start, UUID.randomUUID()));
            Future<?> second = executor.submit(() -> upsertAfter(start, UUID.randomUUID()));
            start.countDown();
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS);

            assertEquals(1, countRows("concurrent-key"));
            assertTrue(repository.findValidRunId("concurrent-key").isPresent());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void deletesOnlyKeysOlderThanFortyEightHours() {
        Instant reference = Instant.now();
        insertRow("expired-key", UUID.randomUUID(), reference.minus(Duration.ofHours(49)));
        insertRow("retained-key", UUID.randomUUID(), reference.minus(Duration.ofHours(47)));

        assertEquals(1, repository.deleteExpired());
        assertEquals(0, countRows("expired-key"));
        assertEquals(1, countRows("retained-key"));
    }

    private void upsertAfter(CountDownLatch start, UUID runId) {
        try {
            start.await(2, TimeUnit.SECONDS);
            repository.upsert("concurrent-key", jobDefinitionId, runId);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    private void insertRow(String key, UUID runId, Instant createdAt) {
        jdbcTemplate.update("""
                INSERT INTO run_trigger_idempotency
                    (idempotency_key, job_definition_id, run_id, created_at)
                VALUES (:key, :jobDefinitionId, :runId, :createdAt)
                """, new MapSqlParameterSource()
                .addValue("key", key)
                .addValue("jobDefinitionId", jobDefinitionId)
                .addValue("runId", runId)
                .addValue("createdAt", Timestamp.from(createdAt)));
    }

    private int countRows(String key) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM run_trigger_idempotency WHERE idempotency_key = :key",
                Map.of("key", key), Integer.class);
    }
}
