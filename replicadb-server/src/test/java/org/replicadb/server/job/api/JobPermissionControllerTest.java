package org.replicadb.server.job.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
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
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.UUID;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class JobPermissionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private JobPermissionRepository jobPermissionRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_permission, job_run, job_definition, app_user CASCADE", Map.of());
    }

    @Test
    @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000019", username = "edit-owner",
            role = GlobalRole.OPERATOR)
    void editHolderCanReplaceListAndRevokePermissions() throws Exception {
        UUID ownerId = UUID.fromString("00000000-0000-0000-0000-000000000019");
        appUserRepository.insert(new AppUser(ownerId, "edit-owner", "hash", GlobalRole.OPERATOR, true, null, null));
        AppUser first = appUserRepository.insert(user("permission-first"));
        AppUser second = appUserRepository.insert(user("permission-second"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("permission-job"));
        jobPermissionRepository.grant(definition.id(), ownerId, JobPermissionType.EDIT);

        replace(definition.id(), first.id(), "[\"VIEW\",\"EXECUTE\"]");
        replace(definition.id(), first.id(), "[\"VIEW\"]");
        replace(definition.id(), second.id(), "[\"EDIT\"]");

        mockMvc.perform(get(path(definition.id())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3));
        assertTrue(jobPermissionRepository.hasPermission(definition.id(), first.id(), JobPermissionType.VIEW));
        assertFalse(jobPermissionRepository.hasPermission(definition.id(), first.id(), JobPermissionType.EXECUTE));

        mockMvc.perform(delete(path(definition.id()) + "/" + second.id()).with(csrf()))
                .andExpect(status().isNoContent());
        mockMvc.perform(delete(path(definition.id()) + "/" + second.id()).with(csrf()))
                .andExpect(status().isNoContent());
    }

    @Test
    @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000020", username = "view-owner",
            role = GlobalRole.VIEWER)
    void viewHolderCannotManagePermissions() throws Exception {
        UUID ownerId = UUID.fromString("00000000-0000-0000-0000-000000000020");
        appUserRepository.insert(new AppUser(ownerId, "view-owner", "hash", GlobalRole.VIEWER, true, null, null));
        AppUser target = appUserRepository.insert(user("permission-target"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("view-permission-job"));
        jobPermissionRepository.grant(definition.id(), ownerId, JobPermissionType.VIEW);

        mockMvc.perform(get(path(definition.id())))
                .andExpect(status().isForbidden());
        mockMvc.perform(put(path(definition.id()) + "/" + target.id()).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"VIEW\"]}"))
                .andExpect(status().isForbidden());
        mockMvc.perform(delete(path(definition.id()) + "/" + target.id()).with(csrf()))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void unknownTargetUserReturnsNotFound() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition("unknown-target-job"));

        mockMvc.perform(put(path(definition.id()) + "/" + UUID.randomUUID()).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"VIEW\"]}"))
                .andExpect(status().isNotFound());
    }

    private void replace(UUID jobDefinitionId, UUID userId, String permissions) throws Exception {
        mockMvc.perform(put(path(jobDefinitionId) + "/" + userId).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":" + permissions + "}"))
                .andExpect(status().isOk());
    }

    private static String path(UUID jobDefinitionId) {
        return "/api/v1/jobs/" + jobDefinitionId + "/permissions";
    }

    private static AppUser user(String username) {
        return new AppUser(null, username, "hash", GlobalRole.VIEWER, true, null, null);
    }

    private static JobDefinition definition(String name) {
        return new JobDefinition(
                null, name, "jdbc:source", null, null, "source_table", null,
                "jdbc:sink", null, null, "sink_table", ReplicationMode.COMPLETE, 1,
                null, null, null, null);
    }
}
