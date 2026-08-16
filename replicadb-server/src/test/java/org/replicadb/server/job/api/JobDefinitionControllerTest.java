package org.replicadb.server.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobDefinitionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE run_trigger_idempotency, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void createsDefinitionWithLocation() throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("created-job", "complete", 1)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/api/v1/jobs/")))
                .andExpect(jsonPath("$.name").value("created-job"))
                .andReturn();

        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        assertTrue(body.get("id").isTextual());
    }

    @Test
    void rejectsBlankNameOnCreateWithProblemDetail() throws Exception {
        mockMvc.perform(post("/api/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("", "complete", 1)))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
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
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(updateJson(null, "incremental")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("update-job"))
                .andExpect(jsonPath("$.sourceConnect").value("jdbc:updated-source"))
                .andExpect(jsonPath("$.mode").value("incremental"))
                .andExpect(jsonPath("$.jobs").value(3))
                .andExpect(jsonPath("$.incrementalWatermarkColumn").value("updated_at"));

        mockMvc.perform(put("/api/v1/jobs/" + inserted.id())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(updateJson("changed-name", "incremental")))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void exposesCompleteModeWarning() throws Exception {
        mockMvc.perform(post("/api/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("warning-job", "complete", 1)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.modeWarning").isNotEmpty());
    }

    private static JobDefinition definition(String name) {
        return new JobDefinition(
                null, name, "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", "source_table", null,
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table", ReplicationMode.COMPLETE,
                1, null, null, null, null);
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
        String nameField = name == null ? "" : "\"name\": \"" + name + "\",\n  ";
        return """
                {
                  %s"sourceConnect": "jdbc:updated-source",
                  "sourceUser": "updated-source-user",
                  "sourcePassword": "${env:UPDATED_SOURCE_PASSWORD}",
                  "sourceTable": "updated_source_table",
                  "sourceWhere": "id > 10",
                  "sinkConnect": "jdbc:updated-sink",
                  "sinkUser": "updated-sink-user",
                  "sinkPassword": "${env:UPDATED_SINK_PASSWORD}",
                  "sinkTable": "updated_sink_table",
                  "mode": "%s",
                  "jobs": 3,
                  "incrementalWatermarkColumn": "updated_at",
                  "initialWatermarkValue": "100"
                }
                """.formatted(nameField, mode);
    }
}
