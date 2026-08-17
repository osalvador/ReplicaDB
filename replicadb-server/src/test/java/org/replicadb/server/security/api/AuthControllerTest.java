package org.replicadb.server.security.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import jakarta.servlet.http.Cookie;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class AuthControllerTest {

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
    void validCredentialsReturnIdentityAndCreateSession() throws Exception {
        String username = uniqueUsername();
        createUser(username, "correct-password", GlobalRole.OPERATOR, true);

        mockMvc.perform(loginRequest(username, "correct-password"))
                .andExpect(status().isOk())
                .andExpect(header().exists("Set-Cookie"))
                .andExpect(jsonPath("$.username").value(username))
                .andExpect(jsonPath("$.role").value("OPERATOR"));

        var events = loginEvents(username, AuditAction.LOGIN_SUCCEEDED);
        assertEquals(1, events.size());
        assertEquals(username, events.get(0).actor().username());
        assertNotNull(events.get(0).actor().sourceAddress());
    }

    @Test
    void wrongPasswordReturnsGenericUnauthorizedProblem() throws Exception {
        String username = uniqueUsername();
        createUser(username, "correct-password", GlobalRole.VIEWER, true);

        mockMvc.perform(loginRequest(username, "wrong-password"))
                .andExpect(status().isUnauthorized())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.detail").value("Invalid credentials"));

        var events = loginEvents(username, AuditAction.LOGIN_FAILED);
        assertEquals(1, events.size());
        assertFalse(events.get(0).detail().toString().contains("wrong-password"));
    }

    @Test
    void sixthFailureIsRateLimitedEvenWithCorrectPassword() throws Exception {
        String username = uniqueUsername();
        createUser(username, "correct-password", GlobalRole.VIEWER, true);

        for (int attempt = 0; attempt < 5; attempt++) {
            mockMvc.perform(loginRequest(username, "wrong-password"))
                    .andExpect(status().isUnauthorized());
        }

        mockMvc.perform(loginRequest(username, "correct-password"))
                .andExpect(status().isTooManyRequests());

        var events = loginEvents(username, AuditAction.LOGIN_FAILED);
        assertEquals(6, events.size());
        assertEquals("THROTTLED", events.get(0).detail().get("reason"));
        }

        @Test
        void unknownUsernameCreatesFailedLoginWithNoUserId() throws Exception {
        String username = uniqueUsername();

        mockMvc.perform(loginRequest(username, "unknown-password"))
            .andExpect(status().isUnauthorized());

        var events = loginEvents(username, AuditAction.LOGIN_FAILED);
        assertEquals(1, events.size());
        assertNull(events.get(0).actor().userId());
    }

    @Test
    void disabledUserCannotLogin() throws Exception {
        String username = uniqueUsername();
        createUser(username, "correct-password", GlobalRole.VIEWER, false);

        mockMvc.perform(loginRequest(username, "correct-password"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.detail").value("Invalid credentials"));
    }

    @Test
    void meReturnsIdentityFromLoginSession() throws Exception {
        String username = uniqueUsername();
        createUser(username, "correct-password", GlobalRole.ADMIN, true);

        MvcResult login = mockMvc.perform(loginRequest(username, "correct-password"))
                .andExpect(status().isOk())
                .andReturn();
        Cookie session = login.getResponse().getCookie("SESSION");
        assertNotNull(session);

        mockMvc.perform(get("/api/v1/auth/me").cookie(session))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.username").value(username))
                .andExpect(jsonPath("$.role").value("ADMIN"));
    }

    @Test
    void meRequiresLogin() throws Exception {
        mockMvc.perform(get("/api/v1/auth/me"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void logoutInvalidatesSession() throws Exception {
        String username = uniqueUsername();
        createUser(username, "correct-password", GlobalRole.VIEWER, true);
        MvcResult login = mockMvc.perform(loginRequest(username, "correct-password"))
                .andExpect(status().isOk())
                .andReturn();
        Cookie session = login.getResponse().getCookie("SESSION");
        assertNotNull(session);

        mockMvc.perform(post("/api/v1/auth/logout").cookie(session).with(csrf()))
                .andExpect(status().isNoContent());
        mockMvc.perform(get("/api/v1/auth/me").cookie(session))
                .andExpect(status().isUnauthorized());

        var events = loginEvents(username, AuditAction.LOGOUT);
        assertEquals(1, events.size());
        assertEquals(username, events.get(0).actor().username());
        assertFalse("anonymous".equals(events.get(0).actor().username()));
    }

    private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder loginRequest(
            String username, String password) {
        return post("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}");
    }

    private void createUser(String username, String password, GlobalRole role, boolean enabled) {
        repository.insert(new AppUser(null, username, passwordEncoder.encode(password), role, enabled, null, null));
    }

    private java.util.List<AuditEvent> loginEvents(String username, AuditAction action) {
        return auditEventRepository.findPage(new AuditEventFilter(null, action,
                AuditResourceType.SESSION, username, null, null), 0, 50);
    }

    private static String uniqueUsername() {
        return "user-" + UUID.randomUUID();
    }
}
