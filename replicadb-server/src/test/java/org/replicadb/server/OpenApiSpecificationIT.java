package org.replicadb.server;

import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class OpenApiSpecificationIT {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @WithMockUser(roles = "ADMIN")
    void exposesApiPathsAsJson() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.paths['/api/v1/jobs']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/datasources']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}']").exists())
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.security").exists())
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.encryptedSecurity")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.DatasourceResponse.properties.safeConnectDisplay")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.DatasourceResponse.properties.encryptedSecurity")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.sourceDatasourceId")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.sinkDatasourceId")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.sourceConnect")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.sourcePassword")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionResponse.properties.sourceDatasource")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionResponse.properties.sinkDatasource")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionResponse.properties.sinkPassword")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.maxAttempts").exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.retryBackoffSeconds").exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.automaticRetryEnabled").exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionResponse.properties.maxAttempts").exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionResponse.properties.retryBackoffSeconds").exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionResponse.properties.automaticRetryEnabled").exists())
                .andExpect(jsonPath("$.components.schemas.JobRunResponse.properties.availableAt").exists())
                .andExpect(jsonPath("$.components.schemas.JobRunResponse.properties.leaseToken").doesNotExist());
    }
}
