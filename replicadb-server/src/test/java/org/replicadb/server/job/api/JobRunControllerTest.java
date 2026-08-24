package org.replicadb.server.job.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.security.WithMockReplicaDbUser;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.security.test.context.support.WithMockUser;
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
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class JobRunControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private JobPermissionRepository jobPermissionRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, job_permission, run_trigger_idempotency, job_run, job_definition, app_user CASCADE",
            Map.of());
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
                    .post("/api/v1/jobs/" + UUID.randomUUID() + "/runs")
                    .with(csrf()))
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
            assertEquals(1, runEvents(AuditAction.RUN_TRIGGERED, null).size());
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
            jobRunRepository.insertPendingNow(definition.id(), null, 1);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/jobs/" + definition.id() + "/runs")
                    .header("Idempotency-Key", "active-conflict")
                    .with(csrf()))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            assertEquals(0, runEvents(AuditAction.RUN_TRIGGERED, null).size());
            }

            @Test
            void cancelsPendingRunAndReturnsModeWarning() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("pending-cancel"));
            JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);

                MvcResult response = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + pending.id() + "/cancel")
                    .with(csrf()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.runId").value(pending.id().toString()))
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.warning").isNotEmpty())
                .andReturn();
            JsonNode responseBody = objectMapper.readTree(response.getResponse().getContentAsString());
            JobRun cancelled = jobRunRepository.findById(pending.id()).orElseThrow();
            assertEquals(JobRunStatus.CANCELLED, cancelled.status());
            assertEquals(responseBody.get("warning").asText(), cancelled.cancellationWarning());
            mockMvc.perform(get("/api/v1/runs/" + pending.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cancellationWarning").value(responseBody.get("warning").asText()))
                .andExpect(jsonPath("$.availableAt").exists())
                .andExpect(jsonPath("$.leaseToken").doesNotExist());
            AuditEvent event = runEvents(AuditAction.RUN_CANCEL_REQUESTED, pending.id()).get(0);
            assertEquals(responseBody.get("warning").asText(), event.detail().get("warning"));
            }

            @Test
            void rejectsCancellationForTerminalRun() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("terminal-cancel"));
            JobRun succeeded = createTerminalRun(definition, JobRunStatus.SUCCEEDED);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + succeeded.id() + "/cancel")
                    .with(csrf()))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            assertNull(jobRunRepository.findById(succeeded.id()).orElseThrow().cancellationWarning());
            assertEquals(0, runEvents(AuditAction.RUN_CANCEL_REQUESTED, null).size());
            }

            @Test
            void returnsDifferentCancellationWarningsForCompleteAndIncrementalModes() throws Exception {
            JobDefinition complete = jobDefinitionRepository.insert(definition("complete-warning"));
            JobDefinition incremental = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("incremental-warning")
                .withMode(ReplicationMode.INCREMENTAL)
                .withIncrementalWatermarkColumn("updated_at")
                .withInitialWatermarkValue("0")
                .build());
            JobDefinition atomic = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("atomic-warning")
                .withMode(ReplicationMode.COMPLETE_ATOMIC)
                .build());
            JobRun completeRun = jobRunRepository.insertPendingNow(complete.id(), null, 1);
            JobRun incrementalRun = jobRunRepository.insertPendingNow(incremental.id(), null, 1);
            JobRun atomicRun = jobRunRepository.insertPendingNow(atomic.id(), null, 1);

            MvcResult completeResponse = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + completeRun.id() + "/cancel")
                .with(csrf()))
                .andExpect(status().isOk())
                .andReturn();
            MvcResult incrementalResponse = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + incrementalRun.id() + "/cancel")
                .with(csrf()))
                .andExpect(status().isOk())
                .andReturn();
            MvcResult atomicResponse = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + atomicRun.id() + "/cancel")
                .with(csrf()))
                .andExpect(status().isOk())
                .andReturn();

            JsonNode completeBody = objectMapper.readTree(completeResponse.getResponse().getContentAsString());
            JsonNode incrementalBody = objectMapper.readTree(incrementalResponse.getResponse().getContentAsString());
            JsonNode atomicBody = objectMapper.readTree(atomicResponse.getResponse().getContentAsString());
            org.junit.jupiter.api.Assertions.assertNotEquals(completeBody.get("warning").asText(),
                incrementalBody.get("warning").asText());
            org.junit.jupiter.api.Assertions.assertNotEquals(completeBody.get("warning").asText(),
                atomicBody.get("warning").asText());
            assertEquals(completeBody.get("warning").asText(),
                jobRunRepository.findById(completeRun.id()).orElseThrow().cancellationWarning());
            assertEquals(incrementalBody.get("warning").asText(),
                jobRunRepository.findById(incrementalRun.id()).orElseThrow().cancellationWarning());
            assertEquals(atomicBody.get("warning").asText(),
                jobRunRepository.findById(atomicRun.id()).orElseThrow().cancellationWarning());
            }

            @Test
            void retriesFailedRunAsANewRun() throws Exception {
            JobDefinition definition = runtimeDefinition("retry-job", ReplicationMode.COMPLETE, 1);
            JobRun failed = jobRunRepository.insertPendingNow(definition.id(), null, 1);
            JobRun running = jobRunRepository.claimNextEligible(failed.id(), "retry-fixture-worker", Duration.ofMinutes(5))
                .orElseThrow();
            jobRunRepository.markFailed(running.id(), running.leaseToken(), 0, 0, "retryable failure");

            MvcResult response = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + failed.id() + "/retry")
                .with(csrf()))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/api/v1/runs/")))
                .andReturn();
            JsonNode body = objectMapper.readTree(response.getResponse().getContentAsString());
            UUID retryId = UUID.fromString(body.get("id").asText());

            org.junit.jupiter.api.Assertions.assertNotEquals(failed.id().toString(), retryId.toString());
            assertEquals(failed.id().toString(), body.get("previousRunId").asText());
            assertEquals(failed.id().toString(), runEvents(AuditAction.RUN_RETRIED, retryId)
                .get(0).detail().get("previousRunId"));
            awaitTerminal(retryId);
            assertEquals(JobRunStatus.RETRY_SCHEDULED, jobRunRepository.findById(failed.id()).orElseThrow().status());
            }

            @Test
            void rejectsRetryForNonFailedRun() throws Exception {
            JobDefinition definition = jobDefinitionRepository.insert(definition("nonfailed-retry"));
            JobRun succeeded = createTerminalRun(definition, JobRunStatus.SUCCEEDED);

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + succeeded.id() + "/retry")
                .with(csrf()))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            assertEquals(0, runEvents(AuditAction.RUN_RETRIED, null).size());
            }

            @Test
            @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000014", username = "view-run-user",
                role = GlobalRole.VIEWER)
            void viewOnlyUserCanReadButCannotExecuteCancelOrRetry() throws Exception {
            UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000014");
            appUserRepository.insert(new AppUser(userId, "view-run-user", "hash", GlobalRole.VIEWER, true, null, null));
            JobDefinition definition = jobDefinitionRepository.insert(definition("view-run-job"));
            jobPermissionRepository.grant(definition.id(), userId, JobPermissionType.VIEW);
            JobRun failed = createTerminalRun(definition, JobRunStatus.FAILED);
            JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 2);

            mockMvc.perform(get("/api/v1/jobs/" + definition.id() + "/runs"))
                .andExpect(status().isOk());
            mockMvc.perform(get("/api/v1/runs/" + failed.id() + "/log"))
                .andExpect(status().isOk());
            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/jobs/" + definition.id() + "/runs")
                    .header("Idempotency-Key", "view-only-trigger")
                    .with(csrf()))
                .andExpect(status().isForbidden());
            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + pending.id() + "/cancel")
                    .with(csrf()))
                .andExpect(status().isForbidden());
            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + failed.id() + "/retry")
                    .with(csrf()))
                .andExpect(status().isForbidden());
            assertEquals(0, runEvents(AuditAction.RUN_CANCEL_REQUESTED, null).size());
            }

            @Test
            @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000015", username = "execute-run-user",
                role = GlobalRole.OPERATOR)
            void executePermissionDoesNotGrantCancel() throws Exception {
            UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000015");
            appUserRepository.insert(new AppUser(userId, "execute-run-user", "hash", GlobalRole.OPERATOR, true, null, null));
            JobDefinition definition = jobDefinitionRepository.insert(definition("execute-only-job"));
            jobPermissionRepository.grant(definition.id(), userId, JobPermissionType.EXECUTE);

            MvcResult triggered = trigger(definition.id(), "execute-only-trigger");
            UUID runId = UUID.fromString(objectMapper.readTree(triggered.getResponse().getContentAsString())
                .get("id").asText());

            mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                    .post("/api/v1/runs/" + runId + "/cancel")
                    .with(csrf()))
                .andExpect(status().isForbidden());
            }

            @Test
            @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000016", username = "history-user",
                role = GlobalRole.VIEWER)
            void globalRunHistoryFiltersByViewPermissionBeforePagination() throws Exception {
            UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000016");
            appUserRepository.insert(new AppUser(userId, "history-user", "hash", GlobalRole.VIEWER, true, null, null));
            JobDefinition first = jobDefinitionRepository.insert(definition("history-visible-a"));
            JobDefinition second = jobDefinitionRepository.insert(definition("history-visible-b"));
            JobDefinition third = jobDefinitionRepository.insert(definition("history-visible-c"));
            JobDefinition hidden = jobDefinitionRepository.insert(definition("history-hidden-d"));
            createTerminalRun(first, JobRunStatus.SUCCEEDED);
            createTerminalRun(second, JobRunStatus.SUCCEEDED);
            createTerminalRun(third, JobRunStatus.SUCCEEDED);
            createTerminalRun(hidden, JobRunStatus.SUCCEEDED);
            jobPermissionRepository.grant(first.id(), userId, JobPermissionType.VIEW);
            jobPermissionRepository.grant(second.id(), userId, JobPermissionType.VIEW);
            jobPermissionRepository.grant(third.id(), userId, JobPermissionType.VIEW);

            mockMvc.perform(get("/api/v1/runs").param("page", "1").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.totalElements").value(3));
            }

    private JobRun createTerminalRun(JobDefinition definition, JobRunStatus status) {
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextEligible(pending.id(), "read-test-worker", Duration.ofMinutes(5))
                .orElseThrow();
        if (status == JobRunStatus.FAILED) {
            jobRunRepository.markFailed(running.id(), running.leaseToken(), 1, 2, "replication failed");
        } else {
            jobRunRepository.markSucceeded(running.id(), running.leaseToken(), 1, 2, null);
        }
        return jobRunRepository.findById(running.id()).orElseThrow();
    }

    private static JobDefinition definition(String name) {
        return JobDefinitionTestFixtures.aJobDefinition().withName(name).build();
    }

    private JobDefinition runtimeDefinition(String name, ReplicationMode mode, int rowCount) throws SQLException {
        Path source = createDatabase(name + "-source.db", rowCount);
        Path sink = createDatabase(name + "-sink.db", 0);
        return jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
            .withName(name)
            .withSourceConnect("jdbc:sqlite:" + source)
            .withSourceTable("orders")
            .withSinkConnect("jdbc:sqlite:" + sink)
            .withSinkTable("orders_copy")
            .withMode(mode)
            .build());
    }

    private MvcResult trigger(UUID jobDefinitionId, String idempotencyKey) throws Exception {
        return mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .post("/api/v1/jobs/" + jobDefinitionId + "/runs")
                        .header("Idempotency-Key", idempotencyKey)
                        .with(csrf()))
                .andExpect(status().isAccepted())
                .andReturn();
    }

    private long countRuns(UUID jobDefinitionId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_run WHERE job_definition_id = :id",
                Map.of("id", jobDefinitionId), Long.class);
    }

    private java.util.List<AuditEvent> runEvents(AuditAction action, UUID resourceId) {
        return auditEventRepository.findPage(new AuditEventFilter(null, action,
                AuditResourceType.JOB_RUN, resourceId == null ? null : resourceId.toString(), null, null), 0, 50);
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
