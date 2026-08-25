package org.replicadb.server;

import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class MetricsEndpointIT {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void authenticatedPrometheusScrapeContainsManagedMetersWithoutSecrets() throws Exception {
        mockMvc.perform(get("/actuator/prometheus").with(user("metrics-reader")))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/plain"))
                .andExpect(content().string(containsString("replicadb_worker")))
                .andExpect(content().string(org.hamcrest.Matchers.not(containsString("leaseToken"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(containsString("password"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(containsString("jdbc:"))));
    }

    @Test
    void authenticatedMetricsIndexIsAvailableWhileEnvironmentStaysClosed() throws Exception {
        mockMvc.perform(get("/actuator/metrics").with(user("metrics-reader")))
                .andExpect(status().isOk())
            .andExpect(content().string(containsString("replicadb.worker.active.slots")));
        mockMvc.perform(get("/actuator/env").with(user("metrics-reader")))
                .andExpect(status().isNotFound());
    }
}
