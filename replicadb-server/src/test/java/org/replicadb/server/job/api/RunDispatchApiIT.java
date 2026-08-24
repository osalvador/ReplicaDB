package org.replicadb.server.job.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.execution.RunExecutionCoordinator;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "replicadb.server.local-execution.enabled=false")
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@WithMockUser(roles = "ADMIN")
class RunDispatchApiIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @MockBean
    private RunExecutionCoordinator executionCoordinator;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE run_trigger_idempotency, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void distributedApiDispatchLeavesPendingWorkForWorkers() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("distributed-api-" + UUID.randomUUID())
                .build());

        mockMvc.perform(post("/api/v1/jobs/" + definition.id() + "/runs")
                        .header("Idempotency-Key", "distributed-api-key")
                        .with(csrf()))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.leaseToken").doesNotExist());

        org.junit.jupiter.api.Assertions.assertEquals(JobRunStatus.PENDING,
                jobRunRepository.findPage(definition.id(), null, 0, 10, null).get(0).status());
        verify(executionCoordinator, never()).submit(any(UUID.class), anyString());
    }
}
