package org.replicadb.server.security.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.WithMockReplicaDbUser;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
class UserControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AppUserRepository repository;

        @Autowired
        private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @BeforeEach
    void clearState() {
                jdbcTemplate.update("TRUNCATE TABLE audit_event, SPRING_SESSION_ATTRIBUTES, SPRING_SESSION, app_user CASCADE",
                                Map.of());
    }

    @Test
        @WithMockReplicaDbUser(userId = "00000000-0000-0000-0000-000000000002",
                        username = "acting-admin", role = GlobalRole.ADMIN)
    void adminCreatesUserWithoutReturningPassword() throws Exception {
        String username = uniqueUsername();
                repository.insert(new AppUser(UUID.fromString("00000000-0000-0000-0000-000000000002"),
                                "acting-admin", "hash", GlobalRole.ADMIN, true, null, null));

        mockMvc.perform(post("/api/v1/users")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createJson(username, "initial-password", "VIEWER")))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/api/v1/users/")))
                .andExpect(jsonPath("$.username").value(username))
                .andExpect(jsonPath("$.role").value("VIEWER"))
                .andExpect(jsonPath("$.passwordHash").doesNotExist())
                .andExpect(jsonPath("$.password").doesNotExist());

        AppUser created = repository.findByUsername(username).orElseThrow();
        var events = userEvents(AuditAction.USER_CREATED, created.id().toString());
        assertEquals(1, events.size());
        assertEquals("acting-admin", events.get(0).actor().username());
        assertEquals(username, events.get(0).detail().get("username"));
        assertEquals("VIEWER", events.get(0).detail().get("role"));
    }

    @Test
    void duplicateUsernameReturnsConflict() throws Exception {
        String username = uniqueUsername();
        repository.insert(user(username, GlobalRole.VIEWER, "hash"));

        mockMvc.perform(post("/api/v1/users")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createJson(username, "initial-password", "VIEWER")))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));

        assertEquals(0, userEvents(AuditAction.USER_CREATED, null).size());
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    void nonAdminCannotUseAnyUserManagementEndpoint() throws Exception {
        AppUser existing = repository.insert(user(uniqueUsername(), GlobalRole.VIEWER, "hash"));

        mockMvc.perform(get("/api/v1/users"))
                .andExpect(status().isForbidden());
        mockMvc.perform(get("/api/v1/users/" + existing.id()))
                .andExpect(status().isForbidden());
        mockMvc.perform(post("/api/v1/users").with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createJson(uniqueUsername(), "password", "VIEWER")))
                .andExpect(status().isForbidden());
        mockMvc.perform(put("/api/v1/users/" + existing.id()).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"role\":\"ADMIN\",\"enabled\":true}"))
                .andExpect(status().isForbidden());
        mockMvc.perform(put("/api/v1/users/" + existing.id() + "/password").with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"newPassword\":\"new-password\"}"))
                .andExpect(status().isForbidden());
    }

    @Test
    void adminUpdatesRoleAndEnabledState() throws Exception {
        AppUser existing = repository.insert(user(uniqueUsername(), GlobalRole.VIEWER, "hash"));

        mockMvc.perform(put("/api/v1/users/" + existing.id()).with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"role\":\"OPERATOR\",\"enabled\":false}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.role").value("OPERATOR"))
                .andExpect(jsonPath("$.enabled").value(false));

        var events = userEvents(AuditAction.USER_UPDATED, existing.id().toString());
        assertEquals(1, events.size());
        assertEquals("OPERATOR", events.get(0).detail().get("role"));
    }

    @Test
    void adminChangesPasswordHash() throws Exception {
        AppUser existing = repository.insert(user(uniqueUsername(), GlobalRole.VIEWER,
                passwordEncoder.encode("old-password")));

        mockMvc.perform(put("/api/v1/users/" + existing.id() + "/password").with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"newPassword\":\"new-password\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.passwordHash").doesNotExist());

        AppUser updated = repository.findById(existing.id()).orElseThrow();
        assertFalse(passwordEncoder.matches("old-password", updated.passwordHash()));
        assertTrue(passwordEncoder.matches("new-password", updated.passwordHash()));

        var events = userEvents(AuditAction.USER_PASSWORD_CHANGED, existing.id().toString());
        assertEquals(1, events.size());
        assertFalse(events.get(0).detail().toString().contains("new-password"));
        assertFalse(events.get(0).detail().toString().contains(updated.passwordHash()));
    }

    @Test
    void listsUsersWithStablePagination() throws Exception {
        repository.insert(user("page-c-" + UUID.randomUUID(), GlobalRole.VIEWER, "hash"));
        repository.insert(user("page-a-" + UUID.randomUUID(), GlobalRole.VIEWER, "hash"));
        repository.insert(user("page-b-" + UUID.randomUUID(), GlobalRole.VIEWER, "hash"));

        mockMvc.perform(get("/api/v1/users").param("page", "1").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.totalElements").value(3));
    }

    private static AppUser user(String username, GlobalRole role, String passwordHash) {
        return new AppUser(null, username, passwordHash, role, true, null, null);
    }

    private static String uniqueUsername() {
        return "managed-" + UUID.randomUUID();
    }

    private static String createJson(String username, String password, String role) {
        return "{\"username\":\"" + username + "\",\"password\":\"" + password
                + "\",\"role\":\"" + role + "\"}";
    }

        private java.util.List<AuditEvent> userEvents(AuditAction action, String resourceId) {
                return auditEventRepository.findPage(new AuditEventFilter(null, action,
                                AuditResourceType.USER, resourceId, null, null), 0, 50);
        }
}
