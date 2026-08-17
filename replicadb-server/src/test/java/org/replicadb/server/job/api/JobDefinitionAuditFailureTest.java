package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class JobDefinitionAuditFailureTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @MockBean
    private AuditEventRepository auditEventRepository;

    @Test
    void auditFailureDoesNotChangeCreateResponseOrPersistence() throws Exception {
        jdbcTemplate.update("TRUNCATE TABLE job_permission, run_trigger_idempotency, job_run, job_definition, app_user CASCADE",
                Map.of());
        doThrow(new RuntimeException("audit insert failed"))
                .when(auditEventRepository).insert(any(AuditEvent.class));

        MvcResult result = mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("audit-failure-job")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("audit-failure-job"))
                .andReturn();

        UUID jobId = UUID.fromString(new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(result.getResponse().getContentAsString()).get("id").asText());
        JobDefinition persisted = jobDefinitionRepository.findById(jobId).orElse(null);
        assertTrue(persisted != null);
    }

    private static String jobJson(String name) {
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
                  "mode": "complete",
                  "jobs": 1
                }
                """.formatted(name);
    }
}
