package org.replicadb.server.job.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobRunControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE run_trigger_idempotency, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void listsRunsForAJobAndPaginates() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition("job-runs"));
        createTerminalRun(definition, JobRunStatus.SUCCEEDED);
        createTerminalRun(definition, JobRunStatus.FAILED);

        mockMvc.perform(get("/api/v1/jobs/" + definition.id() + "/runs")
                        .param("page", "1").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.page").value(1));
    }

    @Test
    void filtersRunsByStatusCaseInsensitively() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition("job-status"));
        createTerminalRun(definition, JobRunStatus.SUCCEEDED);
        createTerminalRun(definition, JobRunStatus.FAILED);

        mockMvc.perform(get("/api/v1/runs").param("status", "failed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].status").value("FAILED"))
                .andExpect(jsonPath("$.totalElements").value(1));

        mockMvc.perform(get("/api/v1/runs").param("status", "not_a_real_status"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value("Unknown run status: not_a_real_status"));
    }

    @Test
    void getsRunAndItsPersistedLogExcerpt() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition("job-log"));
        JobRun failed = createTerminalRun(definition, JobRunStatus.FAILED);
        JobRun succeeded = createTerminalRun(definition, JobRunStatus.SUCCEEDED);

        mockMvc.perform(get("/api/v1/runs/" + failed.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"));
        mockMvc.perform(get("/api/v1/runs/" + failed.id() + "/log"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.runId").value(failed.id().toString()))
                .andExpect(jsonPath("$.excerpt").value("replication failed"));
        mockMvc.perform(get("/api/v1/runs/" + succeeded.id() + "/log"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.excerpt").value(""));

        mockMvc.perform(get("/api/v1/runs/" + UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

            @Test
            void requiresAnIdempotencyKeyToTriggerARun() throws Exception {
            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/jobs/" + UUID.randomUUID() + "/runs"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            }

            @Test
            void replaysTheSameRunForARecentIdempotencyKey() throws Exception {
            JobDefinition definition = runtimeDefinition("trigger-job", ReplicationMode.COMPLETE, 2);

            MvcResult first = trigger(definition.id(), "same-key");
            MvcResult replay = trigger(definition.id(), "same-key");
            JsonNode firstBody = objectMapper.readTree(first.getResponse().getContentAsString());
            JsonNode replayBody = objectMapper.readTree(replay.getResponse().getContentAsString());

            org.junit.jupiter.api.Assertions.assertEquals(firstBody.get("id").asText(), replayBody.get("id").asText());
            assertEquals(1, countRuns(definition.id()));
            awaitTerminal(UUID.fromString(firstBody.get("id").asText()));
            }

            @Test
            void expiredIdempotencyKeyCreatesANewRun() throws Exception {
            JobDefinition definition = runtimeDefinition("expired-key-job", ReplicationMode.COMPLETE, 1);
            UUID staleRunId = UUID.randomUUID();
            jdbcTemplate.update("""
                INSERT INTO run_trigger_idempotency
                    (idempotency_key, job_definition_id, run_id, created_at)
                VALUES (:key, :jobDefinitionId, :runId, :createdAt)
                """, new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
                .addValue("key", "expired-key")
                .addValue("jobDefinitionId", definition.id())
                .addValue("runId", staleRunId)
                .addValue("createdAt", Timestamp.from(Instant.now().minus(Duration.ofHours(25)))));

            MvcResult result = trigger(definition.id(), "expired-key");
            JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());

            org.junit.jupiter.api.Assertions.assertNotEquals(staleRunId.toString(), body.get("id").asText());
            awaitTerminal(UUID.fromString(body.get("id").asText()));
            }

            @Test
            void rejectsTriggerWhenTheJobAlreadyHasAnActiveRun() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("active-job"));
            jobRunRepository.insertPending(definition.id(), null, 1);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/jobs/" + definition.id() + "/runs")
                    .header("Idempotency-Key", "active-conflict"))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            }

            @Test
            void cancelsPendingRunAndReturnsModeWarning() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("pending-cancel"));
            JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + pending.id() + "/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.runId").value(pending.id().toString()))
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.warning").isNotEmpty());
            assertEquals(JobRunStatus.CANCELLED, jobRunRepository.findById(pending.id()).orElseThrow().status());
            }

            @Test
            void rejectsCancellationForTerminalRun() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("terminal-cancel"));
            JobRun succeeded = createTerminalRun(definition, JobRunStatus.SUCCEEDED);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + succeeded.id() + "/cancel"))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            }

            @Test
            void returnsDifferentCancellationWarningsForCompleteAndIncrementalModes() throws Exception {
            JobDefinition complete = jobDefinitionRepository.insert(definition("complete-warning"));
            JobDefinition incremental = jobDefinitionRepository.insert(new JobDefinition(
                null, "incremental-warning", "jdbc:source", null, null, "source_table", null,
                "jdbc:sink", null, null, "sink_table", ReplicationMode.INCREMENTAL, 1,
                "updated_at", "0", null, null));
            JobRun completeRun = jobRunRepository.insertPending(complete.id(), null, 1);
            JobRun incrementalRun = jobRunRepository.insertPending(incremental.id(), null, 1);

            MvcResult completeResponse = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + completeRun.id() + "/cancel"))
                .andExpect(status().isOk())
                .andReturn();
            MvcResult incrementalResponse = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + incrementalRun.id() + "/cancel"))
                .andExpect(status().isOk())
                .andReturn();

            JsonNode completeBody = objectMapper.readTree(completeResponse.getResponse().getContentAsString());
            JsonNode incrementalBody = objectMapper.readTree(incrementalResponse.getResponse().getContentAsString());
            org.junit.jupiter.api.Assertions.assertNotEquals(completeBody.get("warning").asText(),
                incrementalBody.get("warning").asText());
            }

            @Test
            void retriesFailedRunAsANewRun() throws Exception {
            JobDefinition definition = runtimeDefinition("retry-job", ReplicationMode.COMPLETE, 1);
            JobRun failed = jobRunRepository.insertPending(definition.id(), null, 1);
            JobRun running = jobRunRepository.claimById(failed.id(), "retry-fixture-worker", Duration.ofMinutes(5))
                .orElseThrow();
            jobRunRepository.markFailed(running.id(), 0, 0, "retryable failure");

            MvcResult response = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + failed.id() + "/retry"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/api/v1/runs/")))
                .andReturn();
            JsonNode body = objectMapper.readTree(response.getResponse().getContentAsString());
            UUID retryId = UUID.fromString(body.get("id").asText());

            org.junit.jupiter.api.Assertions.assertNotEquals(failed.id().toString(), retryId.toString());
            assertEquals(failed.id().toString(), body.get("previousRunId").asText());
            awaitTerminal(retryId);
            assertEquals(JobRunStatus.RETRY_SCHEDULED, jobRunRepository.findById(failed.id()).orElseThrow().status());
            }

            @Test
            void rejectsRetryForNonFailedRun() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("nonfailed-retry"));
            JobRun succeeded = createTerminalRun(definition, JobRunStatus.SUCCEEDED);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + succeeded.id() + "/retry"))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            }

    private JobRun createTerminalRun(JobDefinition definition, JobRunStatus status) {
        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimById(pending.id(), "read-test-worker", Duration.ofMinutes(5))
                .orElseThrow();
        if (status == JobRunStatus.FAILED) {
            jobRunRepository.markFailed(running.id(), 1, 2, "replication failed");
        } else {
            jobRunRepository.markSucceeded(running.id(), 1, 2, null);
        }
        return jobRunRepository.findById(running.id()).orElseThrow();
    }

    private static JobDefinition definition(String name) {
        return new JobDefinition(
                null, name, "jdbc:source", null, null, "source_table", null,
                "jdbc:sink", null, null, "sink_table", ReplicationMode.COMPLETE, 1,
                null, null, null, null);
    }

    private JobDefinition runtimeDefinition(String name, ReplicationMode mode, int rowCount) throws SQLException {
        Path source = createDatabase(name + "-source.db", rowCount);
        Path sink = createDatabase(name + "-sink.db", 0);
        return jobDefinitionRepository.insert(new JobDefinition(
                null, name, "jdbc:sqlite:" + source, null, null, "orders", null,
                "jdbc:sqlite:" + sink, null, null, "orders_copy", mode, 1,
                null, null, null, null));
    }

    private MvcResult trigger(UUID jobDefinitionId, String idempotencyKey) throws Exception {
        return mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .post("/api/v1/jobs/" + jobDefinitionId + "/runs")
                        .header("Idempotency-Key", idempotencyKey))
                .andExpect(status().isAccepted())
                .andReturn();
    }

    private long countRuns(UUID jobDefinitionId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_run WHERE job_definition_id = :id",
                Map.of("id", jobDefinitionId), Long.class);
    }

    private JobRun awaitTerminal(UUID runId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            JobRun run = jobRunRepository.findById(runId).orElseThrow();
            if (run.status().isTerminal()) {
                return run;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Run did not reach a terminal state: " + runId);
    }

    private Path createDatabase(String filename, int rowCount) throws SQLException {
        Path database = tempDirectory.resolve(filename);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, payload TEXT)");
            statement.execute("CREATE TABLE orders_copy (id INTEGER PRIMARY KEY, payload TEXT)");
            for (int index = 1; index <= rowCount; index++) {
                statement.execute("INSERT INTO orders (id, payload) VALUES (" + index + ", 'payload-" + index + "')");
            }
        }
        return database;
    }
}
