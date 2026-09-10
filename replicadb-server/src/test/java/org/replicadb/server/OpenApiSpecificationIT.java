package org.replicadb.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${replicadb.openapi.output:}")
    private String outputPath;

    @Test
        void exposesPublicContractMetadataAndApiPathsAsJson() throws Exception {
        MvcResult result = mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.info.title").value("ReplicaDB Server API"))
            .andExpect(jsonPath("$.info.version").value("v1"))
            .andExpect(jsonPath("$.tags[*].name", hasItems(
                "Authentication", "Dashboard", "Datasources", "Datasource permissions",
                "Jobs", "Job permissions", "Schedules", "Runs", "Users", "Audit")))
            .andExpect(jsonPath("$.components.securitySchemes.sessionCookie.name").value("JSESSIONID"))
            .andExpect(jsonPath("$.components.securitySchemes.sessionCookie.in").value("cookie"))
            .andExpect(jsonPath("$.components.securitySchemes.csrfHeader.name").value("X-XSRF-TOKEN"))
            .andExpect(jsonPath("$.components.securitySchemes.csrfHeader.in").value("header"))
            .andExpect(jsonPath("$.components.schemas.ProblemDetail.properties.status").exists())
            .andExpect(jsonPath("$.components.responses.BadRequestProblem.content['application/problem+json']").exists())
            .andExpect(jsonPath("$.components.responses.UnauthorizedProblem.content['application/problem+json']").exists())
            .andExpect(jsonPath("$.components.responses.ForbiddenProblem.content['application/problem+json']").exists())
            .andExpect(jsonPath("$.components.responses.NotFoundProblem.content['application/problem+json']").exists())
            .andExpect(jsonPath("$.components.responses.ConflictProblem.content['application/problem+json']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs'].post.operationId").value("createJobDefinition"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs'].get.operationId").value("listJobDefinitions"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs'].get.tags[0]").value("Jobs"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs'].get.parameters[?(@.name == 'page')].schema.default").value("0"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs'].get.parameters[?(@.name == 'size')].schema.default").value("50"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs'].get.parameters[?(@.name == 'size')].schema.maximum").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{id}'].get.operationId").value("getJobDefinition"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{id}'].put.operationId").value("updateJobDefinition"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{id}'].delete.operationId").value("deleteJobDefinition"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{id}'].delete.responses['204']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{id}'].delete.responses['409'].$ref").value("#/components/responses/ConflictProblem"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].get.operationId").value("listJobRunsForJob"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.operationId").value("triggerJobRun"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.parameters[?(@.name == 'Idempotency-Key')].required").value(true))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.parameters[?(@.name == 'Idempotency-Key')].schema.maxLength").value(255))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.responses['202']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.responses['409'].$ref").value("#/components/responses/ConflictProblem"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.security[0].sessionCookie").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.security[0].csrfHeader").exists())
                .andExpect(jsonPath("$.paths['/api/v1/runs'].get.operationId").value("listJobRuns"))
                .andExpect(jsonPath("$.paths['/api/v1/runs'].get.security[0].sessionCookie").exists())
                .andExpect(jsonPath("$.paths['/api/v1/runs'].get.security[0].csrfHeader").doesNotExist())
                .andExpect(jsonPath("$.paths['/api/v1/runs/{id}'].get.operationId").value("getJobRun"))
                .andExpect(jsonPath("$.paths['/api/v1/runs/{id}/log'].get.operationId").value("getJobRunLog"))
                .andExpect(jsonPath("$.paths['/api/v1/runs/{id}/cancel'].post.operationId").value("cancelJobRun"))
                .andExpect(jsonPath("$.paths['/api/v1/runs/{id}/retry'].post.operationId").value("retryJobRun"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/schedule'].put.operationId").value("upsertJobSchedule"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/schedule'].get.operationId").value("getJobSchedule"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/schedule'].delete.operationId").value("deleteJobSchedule"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/schedule'].delete.responses['204']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/dashboard/summary'].get.operationId").value("getDashboardSummary"))
                .andExpect(jsonPath("$.paths['/api/v1/dashboard/summary'].get.tags[0]").value("Dashboard"))
                .andExpect(jsonPath("$.paths['/api/v1/dashboard/summary'].get.responses['400'].$ref").value("#/components/responses/BadRequestProblem"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources'].post.operationId").value("createDatasource"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources'].get.operationId").value("listDatasources"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources'].get.parameters[?(@.name == 'role')].schema.enum[0]").value("source"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources'].get.parameters[?(@.name == 'role')].schema.enum[1]").value("sink"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}'].get.operationId").value("getDatasource"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}'].put.operationId").value("updateDatasource"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}'].put.description").value(org.hamcrest.Matchers.containsString("clearSecurityKeys")))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}'].delete.operationId").value("deleteDatasource"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}'].delete.responses['204']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{datasourceId}/permissions'].get.operationId").value("listDatasourcePermissions"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{datasourceId}/permissions/{userId}'].put.operationId").value("replaceDatasourcePermissions"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{datasourceId}/permissions/{userId}'].delete.operationId").value("revokeDatasourcePermissions"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{datasourceId}/permissions/{userId}'].delete.responses['204']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/permissions'].get.operationId").value("listJobPermissions"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/permissions/{userId}'].put.operationId").value("replaceJobPermissions"))
                .andExpect(jsonPath("$.paths['/api/v1/jobs/{jobDefinitionId}/permissions/{userId}'].delete.operationId").value("revokeJobPermissions"))
                .andExpect(jsonPath("$.paths['/api/v1/auth/login'].post.operationId").value("login"))
                .andExpect(jsonPath("$.paths['/api/v1/auth/login'].post.security").doesNotExist())
                .andExpect(jsonPath("$.paths['/api/v1/auth/login'].post.responses['429'].$ref").value("#/components/responses/TooManyRequestsProblem"))
                .andExpect(jsonPath("$.paths['/api/v1/auth/csrf'].get.operationId").value("getCsrfToken"))
                .andExpect(jsonPath("$.paths['/api/v1/auth/csrf'].get.security").doesNotExist())
                .andExpect(jsonPath("$.paths['/api/v1/auth/logout'].post.operationId").value("logout"))
                .andExpect(jsonPath("$.paths['/api/v1/auth/logout'].post.responses['204']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/auth/me'].get.operationId").value("getCurrentIdentity"))
                .andExpect(jsonPath("$.paths['/api/v1/users'].post.operationId").value("createUser"))
                .andExpect(jsonPath("$.paths['/api/v1/users'].post.responses['201']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/users'].post.responses['409'].$ref").value("#/components/responses/ConflictProblem"))
                .andExpect(jsonPath("$.paths['/api/v1/users'].get.operationId").value("listUsers"))
                .andExpect(jsonPath("$.paths['/api/v1/users/{id}'].get.operationId").value("getUser"))
                .andExpect(jsonPath("$.paths['/api/v1/users/{id}'].put.operationId").value("updateUser"))
                .andExpect(jsonPath("$.paths['/api/v1/users/{id}/password'].put.operationId").value("updateUserPassword"))
                .andExpect(jsonPath("$.paths['/api/v1/audit'].get.operationId").value("listAuditEvents"))
                .andExpect(jsonPath("$.paths['/api/v1/audit'].get.tags[0]").value("Audit"))
                .andExpect(jsonPath("$.paths['/api/v1/datasources']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/datasources/{id}']").exists())
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.security").exists())
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.security.writeOnly").value(true))
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.name.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.name.example").value("Warehouse source"))
                .andExpect(jsonPath("$.components.schemas.DatasourceRequest.properties.encryptedSecurity")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.DatasourceResponse.properties.safeConnectDisplay")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.DatasourceResponse.properties.safeConnectDisplay.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.DatasourceCapabilitiesResponse.properties.sourceModes.items.enum").isArray())
                .andExpect(jsonPath("$.components.schemas.DatasourceResponse.properties.encryptedSecurity")
                    .doesNotExist())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.sourceDatasourceId")
                    .exists())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.mode.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.mode.enum", hasItems("complete", "complete-atomic", "incremental")))
                .andExpect(jsonPath("$.components.schemas.JobDefinitionRequest.properties.jobs.minimum").exists())
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
                .andExpect(jsonPath("$.components.schemas.JobRunResponse.properties.availableAt.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.JobRunResponse.properties.status.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.JobRunResponse.properties.status.enum").isArray())
                .andExpect(jsonPath("$.components.schemas.RunLogResponse.properties.content.maxLength").value(262144))
                .andExpect(jsonPath("$.components.schemas.RunLogResponse.properties.truncated.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.JobScheduleRequest.properties.cronExpression.example").value("0 0 2 * * ?"))
                .andExpect(jsonPath("$.components.schemas.JobScheduleResponse.properties.nextFireTime.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.JobPermissionRequest.properties.permissions.items.enum").isArray())
                .andExpect(jsonPath("$.components.schemas.DatasourcePermissionRequest.properties.permissions.items.enum").isArray())
                .andExpect(jsonPath("$.components.schemas.PageResponseJobRunResponse.properties.size.maximum").exists())
                .andExpect(jsonPath("$.components.schemas.DashboardSummaryResponse.properties.activeRuns.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.DashboardSummaryResponse.properties.averageLatencyMillis.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.LoginRequest.properties.username.example").value("api-operator"))
                .andExpect(jsonPath("$.components.schemas.LoginRequest.properties.password.writeOnly").value(true))
                .andExpect(jsonPath("$.components.schemas.UserRequest.properties.password.writeOnly").value(true))
                .andExpect(jsonPath("$.components.schemas.PasswordUpdate.properties.newPassword.writeOnly").value(true))
                .andExpect(jsonPath("$.components.schemas.UserIdentityResponse.properties.role.enum").isArray())
                .andExpect(jsonPath("$.components.schemas.UserResponse.properties.password").doesNotExist())
                .andExpect(jsonPath("$.components.schemas.AuditEventResponse.properties.action.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.AuditEventResponse.properties.outcome.enum").isArray())
                .andExpect(jsonPath("$.components.schemas.CsrfTokenResponse.properties.headerName.example").value("X-XSRF-TOKEN"))
                .andExpect(jsonPath("$.components.schemas.CancellationResponse.properties.warning.description").isNotEmpty())
                .andExpect(jsonPath("$.components.schemas.JobRunResponse.properties.leaseToken").doesNotExist())
                .andReturn();

            String specification = result.getResponse().getContentAsString();
            assertCompleteOperationMetadata(objectMapper.readTree(specification));
            for (String prohibited : new String[]{
                    "leaseToken", "encryptedSecurity", "sourcePassword", "sinkPassword",
                    "org.replicadb.server", "x-replicadb-local-seed"
            }) {
                assertFalse(specification.contains(prohibited), prohibited);
            }

            if (outputPath != null && !outputPath.isBlank()) {
                Path output = Path.of(outputPath);
                Files.createDirectories(output.toAbsolutePath().getParent());
                Files.writeString(output, objectMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(objectMapper.readTree(result.getResponse().getContentAsString())) + "\n");
            }
    }

    @Test
    void keepsProtectedApiResourcesBehindProblemDetailAuthentication() throws Exception {
        mockMvc.perform(get("/api/v1/jobs"))
                .andExpect(status().isUnauthorized())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.detail").value("Authentication required"));
    }

    private static void assertCompleteOperationMetadata(com.fasterxml.jackson.databind.JsonNode specification) {
        Set<String> methods = Set.of("get", "post", "put", "delete", "patch");
        Set<String> publicOperations = Set.of("login", "getCsrfToken");
        Set<String> domainTags = Set.of("Authentication", "Dashboard", "Datasources",
            "Datasource permissions", "Jobs", "Job permissions", "Schedules", "Runs", "Users", "Audit",
            "Keyring");
        int operationCount = 0;

        Iterator<Map.Entry<String, com.fasterxml.jackson.databind.JsonNode>> paths =
                specification.path("paths").fields();
        while (paths.hasNext()) {
            Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> path = paths.next();
            Iterator<Map.Entry<String, com.fasterxml.jackson.databind.JsonNode>> operations = path.getValue().fields();
            while (operations.hasNext()) {
                Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> method = operations.next();
                if (!methods.contains(method.getKey())) {
                    continue;
                }
                operationCount++;
                com.fasterxml.jackson.databind.JsonNode operation = method.getValue();
                String operationId = operation.path("operationId").asText();
                assertFalse(operationId.isBlank(), path.getKey() + " " + method.getKey() + " operationId");
                assertFalse(operation.path("summary").asText().isBlank(), operationId + " summary");
                assertFalse(operation.path("description").asText().isBlank(), operationId + " description");
                assertTrue(operation.path("tags").isArray() && operation.path("tags").size() == 1,
                        operationId + " tag");
                assertTrue(domainTags.contains(operation.path("tags").get(0).asText()), operationId + " domain tag");
                assertTrue(operation.path("responses").size() > 0, operationId + " responses");
                if (publicOperations.contains(operationId)) {
                    assertFalse(operation.has("security"), operationId + " must be public");
                } else {
                    assertTrue(operation.path("security").isArray() && !operation.path("security").isEmpty(),
                            operationId + " security");
                }
            }
        }
        assertEquals(39, operationCount);
    }
}
