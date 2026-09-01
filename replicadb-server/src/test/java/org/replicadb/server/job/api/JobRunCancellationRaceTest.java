package org.replicadb.server.job.api;

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
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.execution.RunExecutionCoordinator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class JobRunCancellationRaceTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

        @Autowired
        private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @MockBean
    private RunExecutionCoordinator executionCoordinator;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, job_permission, run_trigger_idempotency, job_run, "
                + "job_definition, app_user, datasource_permission, managed_datasource CASCADE",
                Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @Test
    void auditsCancellationWhenRunTerminatesBeforeCancelUpdate() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(
                JobDefinitionTestFixtures.aJobDefinition().withName("cancel-race-job")
                        .withDefaultDatasourceReferences().build());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextEligible(pending.id(), "race-worker", Duration.ofMinutes(5))
                .orElseThrow();

        when(executionCoordinator.requestCancellation(running.id())).thenAnswer(invocation -> {
                        jobRunRepository.markCancelled(running.id(), running.leaseToken(), 0, 0);
            return true;
        });

        mockMvc.perform(post("/api/v1/runs/" + running.id() + "/cancel").with(csrf()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        java.util.List<AuditEvent> events = auditEventRepository.findPage(new AuditEventFilter(null,
                AuditAction.RUN_CANCEL_REQUESTED, AuditResourceType.JOB_RUN, running.id().toString(),
                null, null), 0, 50);
        assertEquals(1, events.size());
        assertEquals("CANCELLED", events.get(0).detail().get("resultingStatus"));
    }
}
