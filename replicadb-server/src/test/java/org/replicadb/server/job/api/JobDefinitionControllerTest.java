package org.replicadb.server.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.security.test.context.support.WithMockUser;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class JobDefinitionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository repository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private JobPermissionRepository jobPermissionRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, job_permission, run_trigger_idempotency, job_run, job_definition, app_user CASCADE",
            Map.of());
    }

    @Test
    void createsDefinitionWithLocation() throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("created-job", "complete", 1)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/api/v1/jobs/")))
                .andExpect(jsonPath("$.name").value("created-job"))
                .andReturn();

        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        assertTrue(body.get("id").isTextual());

        UUID jobId = UUID.fromString(body.get("id").asText());
        var events = jobEvents(AuditAction.JOB_CREATED, jobId);
        assertEquals(1, events.size());
        assertEquals("complete", events.get(0).detail().get("mode"));
        assertEquals("1", events.get(0).detail().get("jobs"));
        assertFalse(events.get(0).detail().keySet().stream()
            .anyMatch(key -> key.matches("(?i).*password.*")));
        assertFalse(events.get(0).detail().values().stream()
            .anyMatch(value -> value.contains("${env:")));
    }

    @Test
    void rejectsBlankNameOnCreateWithProblemDetail() throws Exception {
        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("", "complete", 1)))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

            assertEquals(0, jobEvents(AuditAction.JOB_CREATED, null).size());
    }

    @Test
    void listsDefinitionsWithPagination() throws Exception {
        repository.insert(definition("page-a"));
        repository.insert(definition("page-b"));
        repository.insert(definition("page-c"));

        mockMvc.perform(get("/api/v1/jobs").param("page", "1").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].name").value("page-c"))
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.totalElements").value(3));
    }

    @Test
    void readsDefinitionAndReturnsProblemForUnknownId() throws Exception {
        JobDefinition inserted = repository.insert(definition("read-job"));
        mockMvc.perform(get("/api/v1/jobs/" + inserted.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("read-job"));

        mockMvc.perform(get("/api/v1/jobs/" + UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void updatesMutableFieldsWhileKeepingNameAndCreatedAt() throws Exception {
        JobDefinition inserted = repository.insert(definition("update-job"));

        mockMvc.perform(put("/api/v1/jobs/" + inserted.id())
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(updateJson(null, "incremental")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("update-job"))
                .andExpect(jsonPath("$.sourceConnect").value("jdbc:updated-source"))
                .andExpect(jsonPath("$.mode").value("incremental"))
                .andExpect(jsonPath("$.jobs").value(3))
                .andExpect(jsonPath("$.incrementalWatermarkColumn").value("updated_at"));

            assertEquals(1, jobEvents(AuditAction.JOB_UPDATED, inserted.id()).size());

        mockMvc.perform(put("/api/v1/jobs/" + inserted.id())
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(updateJson("changed-name", "incremental")))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
            assertEquals(1, jobEvents(AuditAction.JOB_UPDATED, inserted.id()).size());
    }

            @Test
            void preservesExistingPasswordsWhenUpdateLeavesEitherBlank() throws Exception {
            JobDefinition inserted = repository.insert(definition("password-update-job"));

            mockMvc.perform(put("/api/v1/jobs/" + inserted.id())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(updateJson(null, "incremental", "", "${env:NEW_SINK_PASSWORD}")))
                .andExpect(status().isOk());

            JobDefinition afterSinkUpdate = repository.findById(inserted.id()).orElseThrow();
            assertEquals("${env:SOURCE_PASSWORD}", afterSinkUpdate.sourcePassword());
            assertEquals("${env:NEW_SINK_PASSWORD}", afterSinkUpdate.sinkPassword());

            mockMvc.perform(put("/api/v1/jobs/" + inserted.id())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(updateJson(null, "incremental", "${env:NEW_SOURCE_PASSWORD}", "")))
                .andExpect(status().isOk());

            JobDefinition afterSourceUpdate = repository.findById(inserted.id()).orElseThrow();
            assertEquals("${env:NEW_SOURCE_PASSWORD}", afterSourceUpdate.sourcePassword());
            assertEquals("${env:NEW_SINK_PASSWORD}", afterSourceUpdate.sinkPassword());

            mockMvc.perform(put("/api/v1/jobs/" + inserted.id())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(updateJson(null, "incremental", "", "")))
                .andExpect(status().isOk());

            JobDefinition afterBlankUpdate = repository.findById(inserted.id()).orElseThrow();
            assertEquals("${env:NEW_SOURCE_PASSWORD}", afterBlankUpdate.sourcePassword());
            assertEquals("${env:NEW_SINK_PASSWORD}", afterBlankUpdate.sinkPassword());
            }

    @Test
    void exposesCompleteModeWarning() throws Exception {
        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("warning-job", "complete", 1)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.modeWarning").isNotEmpty());
    }

            @Test
            void createsAndUpdatesQueryOnlyDefinitionWithAdvancedFields() throws Exception {
            MvcResult result = mockMvc.perform(post("/api/v1/jobs")
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(queryOnlyJobJson("query-job", "select id, name from source_table")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.sourceTable").doesNotExist())
                .andExpect(jsonPath("$.sourceQuery").value("select id, name from source_table"))
                .andExpect(jsonPath("$.sourceColumns").value("id, name"))
                .andExpect(jsonPath("$.sourceAuthMode").value("ActiveDirectoryDefault"))
                .andExpect(jsonPath("$.sourceConnectionParams.clientId").value("[REDACTED]"))
                .andExpect(jsonPath("$.sinkStagingSchema").value("staging"))
                .andExpect(jsonPath("$.sinkStagingTable").value("sink_stage"))
                .andExpect(jsonPath("$.fetchSize").value(250))
                .andExpect(jsonPath("$.bandwidthThrottling").value(512))
                .andExpect(jsonPath("$.verbose").value(true))
                .andReturn();

            UUID jobId = UUID.fromString(objectMapper.readTree(result.getResponse().getContentAsString())
                .get("id").asText());
            mockMvc.perform(put("/api/v1/jobs/" + jobId)
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(queryOnlyJobJson("query-job", "select id from source_table where id > 10")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceQuery").value("select id from source_table where id > 10"))
                .andExpect(jsonPath("$.fetchSize").value(250));
            }

            @Test
            void rejectsDefinitionWithoutSourceTableOrQuery() throws Exception {
            mockMvc.perform(post("/api/v1/jobs")
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(queryOnlyJobJson("invalid-source", null)))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value("source table or query must be configured"));
            }

            @Test
            @WithMockUser(roles = "VIEWER")
            void viewerCannotCreateDefinition() throws Exception {
            mockMvc.perform(post("/api/v1/jobs")
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(jobJson("viewer-create", "complete", 1)))
                .andExpect(status().isForbidden());
            }

            @Test
            @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000011", username = "owner-user")
            void creatorCanReadAndEditOwnDefinition() throws Exception {
            UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000011");
            appUserRepository.insert(new AppUser(userId, "owner-user", "hash", GlobalRole.OPERATOR, true, null, null));

            MvcResult created = mockMvc.perform(post("/api/v1/jobs")
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(jobJson("owner-job", "complete", 1)))
                .andExpect(status().isCreated())
                .andReturn();
            UUID jobId = UUID.fromString(objectMapper.readTree(created.getResponse().getContentAsString()).get("id").asText());

            mockMvc.perform(get("/api/v1/jobs/" + jobId))
                .andExpect(status().isOk());
            mockMvc.perform(put("/api/v1/jobs/" + jobId).with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(updateJson(null, "incremental")))
                .andExpect(status().isOk());
            for (JobPermissionType permission : JobPermissionType.values()) {
                org.junit.jupiter.api.Assertions.assertTrue(
                    jobPermissionRepository.hasPermission(jobId, userId, permission));
            }
            }

            @Test
            @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000012", username = "other-user")
            void userWithoutPermissionCannotReadOrEditDefinition() throws Exception {
            UUID jobId = repository.insert(definition("private-job")).id();
            appUserRepository.insert(new AppUser(UUID.fromString("00000000-0000-0000-0000-000000000012"),
                "other-user", "hash", GlobalRole.OPERATOR, true, null, null));

            mockMvc.perform(get("/api/v1/jobs/" + jobId))
                .andExpect(status().isForbidden());
            mockMvc.perform(put("/api/v1/jobs/" + jobId).with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(updateJson(null, "complete")))
                .andExpect(status().isForbidden());
            }

            @Test
            @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000013", username = "list-user")
            void nonAdminListFiltersVisibleDefinitionsBeforePagination() throws Exception {
            UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000013");
            appUserRepository.insert(new AppUser(userId, "list-user", "hash", GlobalRole.VIEWER, true, null, null));
            JobDefinition first = repository.insert(definition("visible-a"));
            JobDefinition second = repository.insert(definition("visible-b"));
            JobDefinition third = repository.insert(definition("visible-c"));
            repository.insert(definition("hidden-d"));
            jobPermissionRepository.grant(first.id(), userId, JobPermissionType.VIEW);
            jobPermissionRepository.grant(second.id(), userId, JobPermissionType.VIEW);
            jobPermissionRepository.grant(third.id(), userId, JobPermissionType.VIEW);

            mockMvc.perform(get("/api/v1/jobs").param("page", "1").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.totalElements").value(3));
            }

    private static JobDefinition definition(String name) {
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName(name)
            .withSourceUser("source-user")
            .withSourcePassword("${env:SOURCE_PASSWORD}")
            .withSinkUser("sink-user")
            .withSinkPassword("${env:SINK_PASSWORD}")
            .build();
    }

    private static String jobJson(String name, String mode, int jobs) {
        return """
                {
                  "name": "%s",
                  "sourceConnect": "jdbc:source",
                  "sourceUser": "source-user",
                  "sourcePassword": "${env:SOURCE_PASSWORD}",
                  "sourceTable": "source_table",
                  "sinkConnect": "jdbc:sink",
                  "sinkUser": "sink-user",
                  "sinkPassword": "${env:SINK_PASSWORD}",
                  "sinkTable": "sink_table",
                  "mode": "%s",
                  "jobs": %d
                }
                """.formatted(name, mode, jobs);
    }

    private static String updateJson(String name, String mode) {
                return updateJson(name, mode, "${env:UPDATED_SOURCE_PASSWORD}", "${env:UPDATED_SINK_PASSWORD}");
        }

        private static String updateJson(String name, String mode, String sourcePassword, String sinkPassword) {
        String nameField = name == null ? "" : "\"name\": \"" + name + "\",\n  ";
        return """
                {
                  %s"sourceConnect": "jdbc:updated-source",
                  "sourceUser": "updated-source-user",
                                    "sourcePassword": "%s",
                  "sourceTable": "updated_source_table",
                  "sourceWhere": "id > 10",
                  "sinkConnect": "jdbc:updated-sink",
                  "sinkUser": "updated-sink-user",
                                    "sinkPassword": "%s",
                  "sinkTable": "updated_sink_table",
                  "mode": "%s",
                  "jobs": 3,
                  "incrementalWatermarkColumn": "updated_at",
                  "initialWatermarkValue": "100"
                }
                                """.formatted(nameField, sourcePassword, sinkPassword, mode);
    }

        private static String queryOnlyJobJson(String name, String query) {
                String queryField = query == null ? "" : "\"sourceQuery\": \"" + query + "\",\n  ";
                return """
                                {
                                    "name": "%s",
                                    "sourceConnect": "jdbc:source",
                                    "sourceUser": "source-user",
                                    "sourcePassword": "${env:SOURCE_PASSWORD}",
                                    %s"sourceColumns": "id, name",
                                    "sourceAuthMode": "ActiveDirectoryDefault",
                                    "sourceAuthPrincipalId": "source-client",
                                    "sourceConnectionParams": {"clientId": "source-client"},
                                    "sinkConnect": "jdbc:sink",
                                    "sinkUser": "sink-user",
                                    "sinkPassword": "${env:SINK_PASSWORD}",
                                    "sinkTable": "sink_table",
                                    "sinkColumns": "id, name",
                                    "sinkStagingSchema": "staging",
                                    "sinkStagingTable": "sink_stage",
                                    "sinkDisableEscape": true,
                                    "sinkDisableTruncate": true,
                                    "mode": "incremental",
                                    "jobs": 3,
                                    "incrementalWatermarkColumn": "updated_at",
                                    "initialWatermarkValue": "0",
                                    "fetchSize": 250,
                                    "bandwidthThrottling": 512,
                                    "verbose": true
                                }
                                """.formatted(name, queryField);
        }

    private java.util.List<AuditEvent> jobEvents(AuditAction action, UUID resourceId) {
        return auditEventRepository.findPage(new AuditEventFilter(null, action,
                AuditResourceType.JOB_DEFINITION,
                resourceId == null ? null : resourceId.toString(), null, null), 0, 50);
    }
}
