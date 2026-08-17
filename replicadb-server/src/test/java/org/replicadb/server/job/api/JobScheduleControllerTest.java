package org.replicadb.server.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobScheduleRepository;
import org.replicadb.server.job.execution.QuartzScheduleService;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobScheduleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobScheduleRepository jobScheduleRepository;

    @Autowired
    private QuartzScheduleService quartzScheduleService;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_schedule, run_trigger_idempotency, job_run, job_definition CASCADE",
                Map.of());
    }

    @Test
    void putsAValidScheduleAndReturnsItsNextFireTime() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 1 1 ?", "Europe/Madrid", true)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobDefinitionId").value(definition.id().toString()))
                .andExpect(jsonPath("$.timeZone").value("Europe/Madrid"))
                .andExpect(jsonPath("$.nextFireTime").isNotEmpty());
    }

    @Test
    void rejectsAnInvalidCronExpressionWithProblemDetail() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("not-a-cron", "UTC", true)))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void defaultsAnAbsentTimezoneToUtc() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "cronExpression": "0 0 1 1 1 ?",
                                  "enabled": true
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.timeZone").value("UTC"));
    }

    @Test
    void rejectsAnUnknownJobDefinition() throws Exception {
        mockMvc.perform(put(schedulePath(UUID.randomUUID()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 1 1 ?", "UTC", true)))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void disablesAStoredScheduleAndRemovesItsTrigger() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 1 1 ?", "UTC", false)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.enabled").value(false))
                .andExpect(jsonPath("$.nextFireTime").doesNotExist());

        assertTrue(quartzScheduleService.nextFireTime(definition.id()).isEmpty());
    }

    @Test
    void replacesAnExistingScheduleAndChangesItsNextFireTime() throws Exception {
        JobDefinition definition = insertDefinition();
        JsonNode first = objectMapper.readTree(putSchedule(definition.id(),
                scheduleJson("0 0 1 1 1 ?", "UTC", true)).getResponse().getContentAsString());
        JsonNode second = objectMapper.readTree(putSchedule(definition.id(),
                scheduleJson("0 0 1 2 1 ?", "UTC", true)).getResponse().getContentAsString());

        assertTrue(!first.get("nextFireTime").asText().equals(second.get("nextFireTime").asText()));
        assertTrue(jobScheduleRepository.findByJobDefinitionId(definition.id()).isPresent());
    }

    @Test
    void getsTheCurrentScheduleAndReturnsNotFoundBeforeCreation() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(get(schedulePath(definition.id())))
                .andExpect(status().isNotFound());
        putSchedule(definition.id(), scheduleJson("0 0 1 1 1 ?", "UTC", true));

        mockMvc.perform(get(schedulePath(definition.id())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cronExpression").value("0 0 1 1 1 ?"));
    }

    @Test
    void deletesAScheduleAndTheDeleteIsIdempotent() throws Exception {
        JobDefinition definition = insertDefinition();
        putSchedule(definition.id(), scheduleJson("0 0 1 1 1 ?", "UTC", true));

        mockMvc.perform(delete(schedulePath(definition.id())))
                .andExpect(status().isNoContent());
        mockMvc.perform(get(schedulePath(definition.id())))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete(schedulePath(definition.id())))
                .andExpect(status().isNoContent());
    }

    @Test
    void deletingAJobWithoutAScheduleReturnsNoContent() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(delete(schedulePath(definition.id())))
                .andExpect(status().isNoContent());
    }

    private JobDefinition insertDefinition() {
        return jobDefinitionRepository.insert(new JobDefinition(
                null, "schedule-api-job-" + UUID.randomUUID(), "jdbc:source", null,
                "${env:SOURCE_PASSWORD}", "source_table", null, "jdbc:sink", null,
                "${env:SINK_PASSWORD}", "sink_table", ReplicationMode.COMPLETE, 1,
                null, null, null, null));
    }

    private MvcResult putSchedule(UUID jobDefinitionId, String content) throws Exception {
        return mockMvc.perform(put(schedulePath(jobDefinitionId))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(content))
                .andExpect(status().isOk())
                .andReturn();
    }

    private static String schedulePath(UUID jobDefinitionId) {
        return "/api/v1/jobs/" + jobDefinitionId + "/schedule";
    }

    private static String scheduleJson(String cronExpression, String timeZone, boolean enabled) {
        return """
                {
                  "cronExpression": "%s",
                  "timeZone": "%s",
                  "enabled": %s
                }
                """.formatted(cronExpression, timeZone, enabled);
    }
}
