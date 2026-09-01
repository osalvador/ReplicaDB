package org.replicadb.server.job.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.security.WithMockReplicaDbUser;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.DataSourcePermissionType;
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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
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
@WithMockUser(roles = "ADMIN")
class DataSourcePermissionControllerTest {

    private static final UUID TARGET_USER_ID = UUID.fromString(
            "00000000-0000-0000-0000-000000000041");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ManagedDataSourceRepository dataSourceRepository;

    @Autowired
    private DataSourcePermissionRepository permissionRepository;

    @Autowired
    private AppUserRepository userRepository;

    @Autowired
    private SecretProtectionService protectionService;

    @Autowired
    private DatasourceMapper datasourceMapper;

        @Autowired
        private AuditEventRepository auditEventRepository;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, managed_datasource, app_user CASCADE",
                Map.of());
    }

    @Test
    void adminCanReplaceListAndRevokeDatasourcePermissions() throws Exception {
        ManagedDataSource dataSource = dataSource("permission-source");
        userRepository.insert(new AppUser(TARGET_USER_ID, "permission-target", "hash", GlobalRole.VIEWER,
                true, null, null));

        mockMvc.perform(put(permissionPath(dataSource.id(), TARGET_USER_ID)).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"VIEW\",\"USE\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userId").value(TARGET_USER_ID.toString()))
                .andExpect(jsonPath("$.username").value("permission-target"))
                .andExpect(jsonPath("$.permissions.length()").value(2));

        mockMvc.perform(get(permissionPath(dataSource.id(), null)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].username").value("permission-target"))
                .andExpect(jsonPath("$[0].permissions").value(
                        org.hamcrest.Matchers.containsInAnyOrder("VIEW", "USE")));

        mockMvc.perform(delete(permissionPath(dataSource.id(), TARGET_USER_ID)).with(csrf()))
                .andExpect(status().isNoContent());

        assertTrue(permissionRepository.findByDatasourceId(dataSource.id()).isEmpty());
        assertFalse(auditEventRepository.findPage(new AuditEventFilter(null,
                AuditAction.DATASOURCE_PERMISSION_REPLACED, AuditResourceType.DATASOURCE,
                dataSource.id().toString(), null, null), 0, 10).isEmpty());
        assertFalse(auditEventRepository.findPage(new AuditEventFilter(null,
                AuditAction.DATASOURCE_PERMISSION_REVOKED, AuditResourceType.DATASOURCE,
                dataSource.id().toString(), null, null), 0, 10).isEmpty());
    }

    @Test
    void missingDatasourceOrUserReturnsProblemDetail() throws Exception {
        UUID missingDatasource = UUID.randomUUID();
        userRepository.insert(new AppUser(TARGET_USER_ID, "permission-target", "hash", GlobalRole.VIEWER,
                true, null, null));

        mockMvc.perform(put(permissionPath(missingDatasource, TARGET_USER_ID)).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"VIEW\"]}"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

        ManagedDataSource dataSource = dataSource("known-source");
        mockMvc.perform(put(permissionPath(dataSource.id(), UUID.randomUUID())).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"VIEW\"]}"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    @WithMockReplicaDbUser(role = GlobalRole.OPERATOR, userId = "00000000-0000-0000-0000-000000000042",
            username = "operator")
    void nonAdminCannotManageDatasourcePermissions() throws Exception {
        ManagedDataSource dataSource = dataSource("operator-source");

        mockMvc.perform(get("/api/v1/datasources/" + dataSource.id() + "/permissions"))
                .andExpect(status().isForbidden());
        mockMvc.perform(put(permissionPath(dataSource.id(), TARGET_USER_ID)).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"VIEW\"]}"))
                .andExpect(status().isForbidden());
    }

    private ManagedDataSource dataSource(String name) {
        UUID id = UUID.randomUUID();
        DatasourceRequest request = new DatasourceRequest(name, "postgres", Map.of(),
                Map.of("connect", "jdbc:postgresql://host/db"), Set.of());
        var bundle = protectionService.encrypt(id, request.security());
        return dataSourceRepository.insert(datasourceMapper.toDataSource(id, request, request.security(), bundle,
                protectionService.serialize(bundle), null, null));
    }

    private static String permissionPath(UUID datasourceId, UUID userId) {
        String path = "/api/v1/datasources/" + datasourceId + "/permissions";
        return userId == null ? path : path + "/" + userId;
    }
}
