package org.replicadb.server.audit.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class AuditEventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AuditEventRepository repository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, app_user CASCADE", Map.of());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void adminReceivesNewestFirstPaginatedHistory() throws Exception {
        Instant base = Instant.parse("2026-05-01T00:00:00Z");
        repository.insert(event(base, AuditAction.JOB_CREATED, AuditResourceType.JOB_DEFINITION,
                "older", AuditOutcome.SUCCESS));
        repository.insert(event(base.plusSeconds(1), AuditAction.RUN_FAILED, AuditResourceType.JOB_RUN,
                "newer", AuditOutcome.FAILURE));

        mockMvc.perform(get("/api/v1/audit").param("page", "0").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].resourceId").value("newer"))
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.size").value(1));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    void operatorCannotReadAuditHistory() throws Exception {
        mockMvc.perform(get("/api/v1/audit"))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "VIEWER")
    void viewerCannotReadAuditHistory() throws Exception {
        mockMvc.perform(get("/api/v1/audit"))
                .andExpect(status().isForbidden());
    }

    @Test
    void unauthenticatedReadReturnsProblemDetail() throws Exception {
        mockMvc.perform(get("/api/v1/audit"))
                .andExpect(status().isUnauthorized())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.status").value(401));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void filtersByActorActionResourceAndTimeWindow() throws Exception {
        UUID actorUserId = UUID.randomUUID();
        appUserRepository.insert(new AppUser(actorUserId, "audit-filter-user", "hash",
                GlobalRole.VIEWER, true, null, null));
        Instant base = Instant.parse("2026-05-02T00:00:00Z");
        repository.insert(new AuditEvent(UUID.randomUUID(), base, new AuditActor(actorUserId,
                "audit-filter-user", "127.0.0.1"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS, Map.of()));
        repository.insert(event(base.plusSeconds(10), AuditAction.RUN_FAILED, AuditResourceType.JOB_RUN,
                "run-1", AuditOutcome.FAILURE));

        mockMvc.perform(get("/api/v1/audit").param("actorUserId", actorUserId.toString()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.content.length()").value(1));
        mockMvc.perform(get("/api/v1/audit").param("action", "job_created"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.content.length()").value(1));
        mockMvc.perform(get("/api/v1/audit").param("resourceType", "job_definition"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.content.length()").value(1));
        mockMvc.perform(get("/api/v1/audit").param("resourceId", "run-1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.content.length()").value(1));
        mockMvc.perform(get("/api/v1/audit").param("from", base.plusSeconds(5).toString())
                        .param("to", base.plusSeconds(15).toString()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.content.length()").value(1));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void unknownActionReturnsProblemDetail() throws Exception {
        mockMvc.perform(get("/api/v1/audit").param("action", "not-an-action"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.detail").value("Unknown audit action: not-an-action"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void unknownResourceTypeReturnsProblemDetail() throws Exception {
        mockMvc.perform(get("/api/v1/audit").param("resourceType", "not-a-resource"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.detail").value("Unknown audit resource type: not-a-resource"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void clampsPageSizeToMaximum() throws Exception {
        mockMvc.perform(get("/api/v1/audit").param("size", "500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size").value(200));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void rejectsNegativePage() throws Exception {
        mockMvc.perform(get("/api/v1/audit").param("page", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void rejectsInvertedTimeWindow() throws Exception {
        mockMvc.perform(get("/api/v1/audit").param("from", "2026-05-03T00:00:00Z")
                        .param("to", "2026-05-02T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
    }

    private static AuditEvent event(Instant occurredAt, AuditAction action,
                                    AuditResourceType resourceType, String resourceId,
                                    AuditOutcome outcome) {
        return new AuditEvent(UUID.randomUUID(), occurredAt, AuditActor.system("api"), action,
                resourceType, resourceId, outcome, Map.of("source", "test"));
    }
}
