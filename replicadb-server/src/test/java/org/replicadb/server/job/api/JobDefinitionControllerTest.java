package org.replicadb.server.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.security.WithMockReplicaDbUser;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.DataSourcePermissionType;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.replicadb.server.security.persistence.DataSourcePermissionRepository;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.replicadb.server.security.secret.SecretProtectionService;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
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
@WithMockUser(roles = "ADMIN")
class JobDefinitionControllerTest {

    private static final UUID VIEWER_ID = UUID.fromString("00000000-0000-0000-0000-000000000031");
    private static final UUID OPERATOR_ID = UUID.fromString("00000000-0000-0000-0000-000000000032");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private ManagedDataSourceRepository dataSourceRepository;

    @Autowired
    private DataSourcePermissionRepository dataSourcePermissionRepository;

    @Autowired
    private JobPermissionRepository jobPermissionRepository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private DatasourceMapper datasourceMapper;

    @Autowired
    private SecretProtectionService protectionService;

        @Autowired
        private AuditEventRepository auditEventRepository;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, datasource_permission, job_permission, "
                + "run_trigger_idempotency, job_run, job_schedule, job_definition, managed_datasource, app_user CASCADE",
                Map.of());
    }

    @Test
    void createsDatasourceOnlyJobAndReturnsSafeBindingSummaries() throws Exception {
        ManagedDataSource source = dataSource("source", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/source");
        ManagedDataSource sink = dataSource("sink", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/sink");

        MvcResult result = mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("orders", source.id(), sink.id(), "complete", true, true)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/api/v1/jobs/")))
                .andExpect(jsonPath("$.sourceDatasourceId").value(source.id().toString()))
                .andExpect(jsonPath("$.sinkDatasourceId").value(sink.id().toString()))
                .andExpect(jsonPath("$.sourceDatasource.name").value("source"))
                .andExpect(jsonPath("$.sinkDatasource.connectorType").value("postgres"))
                .andExpect(jsonPath("$.sourceDatasource.safeConnectDisplay")
                        .value("jdbc:postgresql://host/source"))
                .andExpect(jsonPath("$.sourceDatasourceUseEnabled").value(true))
                .andExpect(jsonPath("$.sinkDatasourceUseEnabled").value(true))
                .andExpect(jsonPath("$.sourceConnect").doesNotExist())
                .andExpect(jsonPath("$.sourcePassword").doesNotExist())
                .andReturn();

        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        JobDefinition persisted = jobDefinitionRepository.findById(UUID.fromString(body.get("id").asText()))
                .orElseThrow();
        assertEquals(source.id(), persisted.sourceDatasourceId());
        assertEquals(sink.id(), persisted.sinkDatasourceId());
        assertNull(persisted.source().connection());
        assertNull(persisted.sink().connection());
        assertFalse(result.getResponse().getContentAsString().contains("encryptedSecurity"));
    }

    @Test
    void rejectsMissingDatasourceIdsAndLegacyInlinePayloads() throws Exception {
        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "missing-bindings",
                                  "sourceTable": "source_table",
                                  "sinkTable": "sink_table",
                                  "mode": "complete",
                                  "jobs": 1
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "legacy-inline",
                                  "sourceConnect": "jdbc:source",
                                  "sourcePassword": "should-not-be-accepted",
                                  "sourceTable": "source_table",
                                  "sinkConnect": "jdbc:sink",
                                  "sinkTable": "sink_table",
                                  "mode": "complete",
                                  "jobs": 1
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void rejectsRoleAndModeCapabilityMismatches() throws Exception {
        ManagedDataSource sourceOnly = dataSource("source-only", ConnectorType.DENODO,
                "jdbc:denodo://host/source");
        ManagedDataSource postgres = dataSource("postgres", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/db");

        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("wrong-role", postgres.id(), sourceOnly.id(), "complete", true, true)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Datasource cannot be used as a sink"));

        ManagedDataSource kafka = dataSource("kafka", ConnectorType.KAFKA, "kafka://host/topic");
        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("wrong-mode", kafka.id(), postgres.id(), "incremental", true, true)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Datasource cannot be used as a source"));
    }

    @Test
    @WithMockReplicaDbUser(role = GlobalRole.OPERATOR, userId = "00000000-0000-0000-0000-000000000032",
            username = "binding-operator")
    void requiresUseToCreateAndAllowsCreateAfterUseIsGranted() throws Exception {
        ManagedDataSource source = dataSource("operator-source", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/source");
        ManagedDataSource sink = dataSource("operator-sink", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/sink");
        appUserRepository.insert(new AppUser(OPERATOR_ID, "binding-operator", "hash", GlobalRole.OPERATOR,
                true, null, null));

        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("without-use", source.id(), sink.id(), "complete", true, true)))
                .andExpect(status().isForbidden());

        dataSourcePermissionRepository.grant(source.id(), OPERATOR_ID, DataSourcePermissionType.USE);
        dataSourcePermissionRepository.grant(sink.id(), OPERATOR_ID, DataSourcePermissionType.USE);

        mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("with-use", source.id(), sink.id(), "complete", true, true)))
                .andExpect(status().isCreated());
    }

    @Test
    void disablesAndReenablesBindingsWithoutChangingDatasourceSelection() throws Exception {
        ManagedDataSource source = dataSource("toggle-source", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/source");
        ManagedDataSource sink = dataSource("toggle-sink", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/sink");
        UUID jobId = createJob("toggle-job", source.id(), sink.id(), true, true);

        mockMvc.perform(put("/api/v1/jobs/" + jobId)
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson("toggle-job", source.id(), sink.id(), "complete", false, false)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceDatasourceUseEnabled").value(false))
                .andExpect(jsonPath("$.sinkDatasourceUseEnabled").value(false));

        mockMvc.perform(put("/api/v1/jobs/" + jobId)
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJsonWithoutFlags("toggle-job", source.id(), sink.id(), "complete")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceDatasourceUseEnabled").value(false))
                .andExpect(jsonPath("$.sinkDatasourceUseEnabled").value(false));

        JobDefinition persisted = jobDefinitionRepository.findById(jobId).orElseThrow();
        assertEquals(source.id(), persisted.sourceDatasourceId());
        assertEquals(sink.id(), persisted.sinkDatasourceId());
        var bindingEvents = auditEventRepository.findPage(new AuditEventFilter(null,
                AuditAction.JOB_DATASOURCE_BINDING_DISABLED, AuditResourceType.JOB_DEFINITION,
                jobId.toString(), null, null), 0, 10);
        assertEquals(2, bindingEvents.size());
        assertEquals(2, bindingEvents.stream()
                .filter(event -> "source".equals(event.detail().get("side")))
                .count()
                + bindingEvents.stream().filter(event -> "sink".equals(event.detail().get("side"))).count());
        assertTrue(bindingEvents.stream().allMatch(event -> !event.detail().containsKey("password")));
    }

    @Test
    @WithMockReplicaDbUser(role = GlobalRole.VIEWER, userId = "00000000-0000-0000-0000-000000000031",
            username = "job-viewer")
    void doesNotLeakDatasourceSummaryWhenViewerOnlyHasJobPermission() throws Exception {
        ManagedDataSource source = dataSource("hidden-source", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/source");
        ManagedDataSource sink = dataSource("hidden-sink", ConnectorType.POSTGRES,
                "jdbc:postgresql://host/sink");
        JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
                .withName("hidden-bindings")
                .withSourceDatasourceId(source.id())
                .withSinkDatasourceId(sink.id())
                .build();
        UUID jobId = jobDefinitionRepository.insert(definition).id();
        appUserRepository.insert(new AppUser(VIEWER_ID, "job-viewer", "hash", GlobalRole.VIEWER,
                true, null, null));
        jobPermissionRepository.grant(jobId, VIEWER_ID,
                org.replicadb.server.security.domain.JobPermissionType.VIEW);

        MvcResult result = mockMvc.perform(get("/api/v1/jobs/" + jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceDatasourceId").value(source.id().toString()))
                .andExpect(jsonPath("$.sourceDatasource").doesNotExist())
                .andExpect(jsonPath("$.sinkDatasource").doesNotExist())
                .andReturn();

        assertFalse(result.getResponse().getContentAsString().contains("host/source"));
    }

    private UUID createJob(String name, UUID sourceId, UUID sinkId,
                           boolean sourceUseEnabled, boolean sinkUseEnabled) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/jobs")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jobJson(name, sourceId, sinkId, "complete", sourceUseEnabled, sinkUseEnabled)))
                .andExpect(status().isCreated())
                .andReturn();
        return UUID.fromString(objectMapper.readTree(result.getResponse().getContentAsString()).get("id").asText());
    }

    private ManagedDataSource dataSource(String name, ConnectorType connectorType, String connect) {
        UUID id = UUID.randomUUID();
        DatasourceRequest request = new DatasourceRequest(name, connectorType.getWireValue(), Map.of(),
                Map.of("connect", connect), java.util.Set.of());
        var bundle = protectionService.encrypt(id, request.security());
        return dataSourceRepository.insert(datasourceMapper.toDataSource(id, request, request.security(), bundle,
                protectionService.serialize(bundle), null, null));
    }

    private static String jobJson(String name, UUID sourceId, UUID sinkId, String mode,
                                  boolean sourceUseEnabled, boolean sinkUseEnabled) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("sourceDatasourceId", sourceId);
        body.put("sourceDatasourceUseEnabled", sourceUseEnabled);
        body.put("sourceTable", "source_table");
        body.put("sinkDatasourceId", sinkId);
        body.put("sinkDatasourceUseEnabled", sinkUseEnabled);
        body.put("sinkTable", "sink_table");
        body.put("mode", mode);
        body.put("jobs", 1);
        return new ObjectMapper().writeValueAsString(body);
    }

    private static String jobJsonWithoutFlags(String name, UUID sourceId, UUID sinkId, String mode) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("sourceDatasourceId", sourceId);
        body.put("sourceTable", "source_table");
        body.put("sinkDatasourceId", sinkId);
        body.put("sinkTable", "sink_table");
        body.put("mode", mode);
        body.put("jobs", 1);
        return new ObjectMapper().writeValueAsString(body);
    }
}
