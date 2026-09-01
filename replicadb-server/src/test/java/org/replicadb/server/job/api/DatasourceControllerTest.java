package org.replicadb.server.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.security.WithMockReplicaDbUser;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.replicadb.server.security.persistence.DataSourcePermissionRepository;
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
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
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
class DatasourceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ManagedDataSourceRepository dataSourceRepository;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private DatasourceMapper mapper;

    @Autowired
    private SecretProtectionService protectionService;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private DataSourcePermissionRepository permissionRepository;

        @Autowired
        private AuditEventRepository auditEventRepository;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, datasource_permission, job_permission, "
                + "run_trigger_idempotency, job_run, job_definition, managed_datasource, app_user CASCADE",
                Map.of());
    }

    @Test
    void createsReadsAndListsOnlySafeDatasourceMetadata() throws Exception {
        UUID id = createDatasource("orders", "jdbc:postgresql://user:password@host/db", "placeholder-password");

        MvcResult read = mockMvc.perform(get("/api/v1/datasources/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.connectorType").value("postgres"))
                .andExpect(jsonPath("$.safeConnectDisplay").value(
                        "jdbc:postgresql://[REDACTED]@host/db"))
                .andExpect(jsonPath("$.securityConfigured").value(true))
                .andExpect(jsonPath("$.capabilities.sourceCapable").value(true))
                .andExpect(jsonPath("$.capabilities.sinkCapable").value(true))
                .andExpect(jsonPath("$.canView").value(true))
                .andExpect(jsonPath("$.canUse").value(true))
                .andReturn();

        String response = read.getResponse().getContentAsString();
        assertFalse(response.contains("placeholder-password"));
        assertFalse(response.contains("encryptedSecurity"));
        assertFalse(response.contains("keyVersion"));
        List<AuditEvent> events = auditEventRepository.findPage(new AuditEventFilter(null,
                AuditAction.DATASOURCE_CREATED, AuditResourceType.DATASOURCE, id.toString(), null, null), 0, 10);
        assertEquals(1, events.size());
        assertFalse(events.get(0).detail().toString().contains("password"));

        mockMvc.perform(get("/api/v1/datasources").param("page", "0").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.totalElements").value(1));
    }

    @Test
    void preservesBlankSecurityUpdatesAndSupportsExplicitClearing() throws Exception {
        UUID id = createDatasource("orders", "jdbc:postgresql://host/db", "placeholder-password");

        mockMvc.perform(put("/api/v1/datasources/" + id)
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(datasourceJson("orders", "postgres", "jdbc:postgresql://host/db", "", Set.of())))
                .andExpect(status().isOk());

        assertEquals("placeholder-password", storedSecurity(id).get("password"));

        mockMvc.perform(put("/api/v1/datasources/" + id)
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(datasourceJson("orders", "postgres", "jdbc:postgresql://host/db", "",
                                Set.of("password"))))
                .andExpect(status().isOk());

        assertFalse(storedSecurity(id).containsKey("password"));
        assertTrue(storedSecurity(id).containsKey("connect"));
    }

    @Test
    void rejectsConnectorMismatchAndDuplicateNamesAsProblems() throws Exception {
        createDatasource("orders", "jdbc:postgresql://host/db", "placeholder-password");

        mockMvc.perform(post("/api/v1/datasources")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(datasourceJson("sqlite-source", "postgres", "jdbc:sqlite:/tmp/db", null, Set.of())))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

        mockMvc.perform(post("/api/v1/datasources")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(datasourceJson("orders", "postgres", "jdbc:postgresql://other/db", null, Set.of())))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value("Datasource name is already in use"));
    }

    @Test
    void refusesDeletionWhileAJobReferencesDatasource() throws Exception {
        ManagedDataSource dataSource = storedDatasource("referenced", "jdbc:postgresql://host/db");
        jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withSourceDatasourceId(dataSource.id())
                .withSinkDatasourceId(dataSource.id())
                .build());

        MvcResult result = mockMvc.perform(delete("/api/v1/datasources/" + dataSource.id()).with(csrf()))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andReturn();

        assertFalse(result.getResponse().getContentAsString().contains("password"));
        assertTrue(dataSourceRepository.findById(dataSource.id()).isPresent());
        assertTrue(auditEventRepository.findPage(new AuditEventFilter(null, AuditAction.DATASOURCE_DELETED,
                AuditResourceType.DATASOURCE, dataSource.id().toString(), null, null), 0, 10).isEmpty());
    }

        @Test
        void deletesAnUnreferencedDatasource() throws Exception {
                UUID id = createDatasource("deletable", "jdbc:postgresql://host/db", null);

                mockMvc.perform(delete("/api/v1/datasources/" + id).with(csrf()))
                                .andExpect(status().isNoContent());

                assertFalse(dataSourceRepository.findById(id).isPresent());
                assertEquals(1, auditEventRepository.findPage(new AuditEventFilter(null, AuditAction.DATASOURCE_DELETED,
                        AuditResourceType.DATASOURCE, id.toString(), null, null), 0, 10).size());
        }

        @Test
        void rejectsEncryptedDatasourceFields() throws Exception {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("name", "invalid-envelope");
                body.put("connectorType", "postgres");
                body.put("technicalParams", Map.of());
                body.put("security", Map.of("connect", "jdbc:postgresql://host/db"));
                body.put("clearSecurityKeys", List.of());
                body.put("encryptedSecurity", "not-accepted");

                mockMvc.perform(post("/api/v1/datasources")
                                                .with(csrf())
                                                .contentType(MediaType.APPLICATION_JSON)
                                                .content(objectMapper.writeValueAsString(body)))
                                .andExpect(status().isBadRequest())
                                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
        }

    @Test
    @WithMockReplicaDbUser(role = GlobalRole.VIEWER,
            userId = "00000000-0000-0000-0000-000000000021", username = "datasource-viewer")
    void viewerNeedsViewAclAndReceivesNoUseOrEditCapability() throws Exception {
        UUID userId = UUID.fromString("00000000-0000-0000-0000-000000000021");
        appUserRepository.insert(new AppUser(userId, "datasource-viewer", "hash", GlobalRole.VIEWER,
                true, null, null));
        ManagedDataSource dataSource = storedDatasource("private", "jdbc:postgresql://host/db");

        mockMvc.perform(get("/api/v1/datasources/" + dataSource.id()))
                .andExpect(status().isForbidden());

        permissionRepository.grant(dataSource.id(), userId,
                org.replicadb.server.security.domain.DataSourcePermissionType.VIEW);

        mockMvc.perform(get("/api/v1/datasources/" + dataSource.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.canView").value(true))
                .andExpect(jsonPath("$.canUse").value(false))
                .andExpect(jsonPath("$.canEdit").value(false));
    }

    @Test
    @WithMockReplicaDbUser(role = GlobalRole.VIEWER,
            userId = "00000000-0000-0000-0000-000000000022", username = "datasource-creator")
    void nonAdminCannotCreateDatasource() throws Exception {
        mockMvc.perform(post("/api/v1/datasources")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(datasourceJson("forbidden", "postgres", "jdbc:postgresql://host/db", null,
                                Set.of())))
                .andExpect(status().isForbidden());
    }

    private UUID createDatasource(String name, String connect, String password) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/datasources")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(datasourceJson(name, "postgres", connect, password, Set.of())))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith(
                        "/api/v1/datasources/")))
                .andReturn();
        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        return UUID.fromString(body.get("id").asText());
    }

    private ManagedDataSource storedDatasource(String name, String connect) {
        DatasourceRequest request = new DatasourceRequest(name, "postgres", Map.of(),
                Map.of("connect", connect), Set.of());
        UUID id = UUID.randomUUID();
        var bundle = protectionService.encrypt(id, request.security());
        return dataSourceRepository.insert(mapper.toDataSource(id, request, request.security(), bundle,
                protectionService.serialize(bundle), null, null));
    }

    private Map<String, String> storedSecurity(UUID id) {
        ManagedDataSource dataSource = dataSourceRepository.findById(id).orElseThrow();
        return protectionService.decrypt(id,
                protectionService.deserialize(dataSource.encryptedSecurity()));
    }

    private String datasourceJson(String name, String connectorType, String connect,
                                  String password, Set<String> clearSecurityKeys) throws Exception {
        Map<String, String> security = new LinkedHashMap<>();
        security.put("connect", connect);
        if (password != null) {
            security.put("password", password);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("connectorType", connectorType);
        body.put("technicalParams", Map.of());
        body.put("security", security);
        body.put("clearSecurityKeys", List.copyOf(clearSecurityKeys));
        return objectMapper.writeValueAsString(body);
    }
}
