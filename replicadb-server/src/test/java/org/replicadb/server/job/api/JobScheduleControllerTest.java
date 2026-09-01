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
import org.replicadb.server.job.domain.JobSchedule;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.persistence.JobScheduleRepository;
import org.replicadb.server.job.execution.QuartzScheduleService;
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
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class JobScheduleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

        @Autowired
        private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private JobScheduleRepository jobScheduleRepository;

        @Autowired
        private AppUserRepository appUserRepository;

        @Autowired
        private JobPermissionRepository jobPermissionRepository;

    @Autowired
    private QuartzScheduleService quartzScheduleService;

        @Autowired
        private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
                                jdbcTemplate.update("TRUNCATE TABLE audit_event, job_permission, job_schedule, "
                                                + "run_trigger_idempotency, job_run, job_definition, app_user, datasource_permission, "
                                                + "managed_datasource CASCADE",
                Map.of());
                managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
                managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @Test
    void putsAValidScheduleAndReturnsItsNextFireTime() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 1 1 ?", "Europe/Madrid", true)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobDefinitionId").value(definition.id().toString()))
                .andExpect(jsonPath("$.timeZone").value("Europe/Madrid"))
                .andExpect(jsonPath("$.nextFireTime").isNotEmpty());

        var events = scheduleEvents(AuditAction.JOB_SCHEDULE_UPSERTED, definition.id());
        assertEquals(1, events.size());
        assertEquals("0 0 1 1 1 ?", events.get(0).detail().get("cronExpression"));
        assertEquals("Europe/Madrid", events.get(0).detail().get("timeZone"));
    }

    @Test
    void rejectsAnInvalidCronExpressionWithProblemDetail() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("not-a-cron", "UTC", true)))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
        assertTrue(scheduleEvents(AuditAction.JOB_SCHEDULE_UPSERTED, definition.id()).isEmpty());
    }

    @Test
    void defaultsAnAbsentTimezoneToUtc() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "cronExpression": "0 0 1 1 1 ?",
                                  "enabled": true
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.timeZone").value("UTC"));
        assertEquals("UTC", scheduleEvents(AuditAction.JOB_SCHEDULE_UPSERTED, definition.id())
                .get(0).detail().get("timeZone"));
    }

    @Test
    void rejectsAnUnknownJobDefinition() throws Exception {
        mockMvc.perform(put(schedulePath(UUID.randomUUID())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 1 1 ?", "UTC", true)))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void disablesAStoredScheduleAndRemovesItsTrigger() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(put(schedulePath(definition.id())).with(csrf())
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

        mockMvc.perform(delete(schedulePath(definition.id())).with(csrf()))
                .andExpect(status().isNoContent());
        assertEquals(1, scheduleEvents(AuditAction.JOB_SCHEDULE_DELETED, definition.id()).size());
        mockMvc.perform(get(schedulePath(definition.id())))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete(schedulePath(definition.id())).with(csrf()))
                .andExpect(status().isNoContent());
        assertEquals(2, scheduleEvents(AuditAction.JOB_SCHEDULE_DELETED, definition.id()).size());
    }

    @Test
    void deletingAJobWithoutAScheduleReturnsNoContent() throws Exception {
        JobDefinition definition = insertDefinition();

        mockMvc.perform(delete(schedulePath(definition.id())).with(csrf()))
                .andExpect(status().isNoContent());
        assertEquals(1, scheduleEvents(AuditAction.JOB_SCHEDULE_DELETED, definition.id()).size());
    }

    @Test
    @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000017", username = "schedule-view-user",
            role = GlobalRole.VIEWER)
    void viewPermissionAllowsReadButNotScheduleMutation() throws Exception {
        UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000017");
        appUserRepository.insert(new AppUser(userId, "schedule-view-user", "hash", GlobalRole.VIEWER, true, null, null));
        JobDefinition definition = insertDefinition();
        jobPermissionRepository.grant(definition.id(), userId, JobPermissionType.VIEW);
        jobScheduleRepository.upsert(new JobSchedule(definition.id(), "0 0 1 1 1 ?", "UTC", true, null, null));

        mockMvc.perform(get(schedulePath(definition.id())))
                .andExpect(status().isOk());
        mockMvc.perform(put(schedulePath(definition.id())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 2 1 ?", "UTC", true)))
                .andExpect(status().isForbidden());
        mockMvc.perform(delete(schedulePath(definition.id())).with(csrf()))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000018", username = "schedule-edit-user",
            role = GlobalRole.OPERATOR)
    void editPermissionAllowsScheduleReplacementAndRemoval() throws Exception {
        UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000018");
        appUserRepository.insert(new AppUser(userId, "schedule-edit-user", "hash", GlobalRole.OPERATOR, true, null, null));
        JobDefinition definition = insertDefinition();
        jobPermissionRepository.grant(definition.id(), userId, JobPermissionType.EDIT);

        mockMvc.perform(put(schedulePath(definition.id())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(scheduleJson("0 0 1 1 1 ?", "UTC", true)))
                .andExpect(status().isOk());
        mockMvc.perform(delete(schedulePath(definition.id())).with(csrf()))
                .andExpect(status().isNoContent());
    }

    private JobDefinition insertDefinition() {
        return jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("schedule-api-job-" + UUID.randomUUID())
                                .withDefaultDatasourceReferences()
                .build());
    }

    private MvcResult putSchedule(UUID jobDefinitionId, String content) throws Exception {
        return mockMvc.perform(put(schedulePath(jobDefinitionId)).with(csrf())
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

        private java.util.List<AuditEvent> scheduleEvents(AuditAction action, UUID jobDefinitionId) {
                return auditEventRepository.findPage(new AuditEventFilter(null, action,
                                AuditResourceType.JOB_DEFINITION, jobDefinitionId.toString(), null, null), 0, 50);
        }
}
