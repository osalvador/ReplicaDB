package org.replicadb.server.security.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.tags.Tag;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.Set;

@Configuration
@Profile("api")
public class OpenApiConfiguration {

    static final String SESSION_COOKIE_SCHEME = "sessionCookie";
    static final String CSRF_HEADER_SCHEME = "csrfHeader";
    private static final String PROBLEM_SCHEMA = "#/components/schemas/ProblemDetail";

    @Bean
    OpenAPI replicaDbServerApi() {
        Components components = new Components()
                .addSecuritySchemes(SESSION_COOKIE_SCHEME, new SecurityScheme()
                        .type(SecurityScheme.Type.APIKEY)
                        .in(SecurityScheme.In.COOKIE)
                        .name("JSESSIONID")
                        .description("Session cookie issued by an authenticated, same-origin ReplicaDB Server deployment."))
                .addSecuritySchemes(CSRF_HEADER_SCHEME, new SecurityScheme()
                        .type(SecurityScheme.Type.APIKEY)
                        .in(SecurityScheme.In.HEADER)
                        .name("X-XSRF-TOKEN")
                        .description("CSRF header for protected state-changing requests. Its value matches the XSRF-TOKEN cookie."));

        return new OpenAPI()
                .info(new Info()
                        .title("ReplicaDB Server API")
                        .version("v1")
                        .description("Static contract for the authenticated ReplicaDB Server control plane. "
                                + "Use it against a same-origin deployment that owns the session and CSRF cookies."))
                .tags(List.of(
                        tag("Authentication", "Session, CSRF, and current identity operations."),
                        tag("Dashboard", "Permission-aware operational summaries."),
                        tag("Datasources", "Managed source and sink connection profiles."),
                        tag("Datasource permissions", "Datasource resource grants."),
                        tag("Jobs", "Managed replication definitions."),
                        tag("Job permissions", "Job resource grants."),
                        tag("Schedules", "Recurring Quartz schedules for jobs."),
                        tag("Runs", "Run dispatch, state, diagnostics, cancellation, and retry."),
                        tag("Users", "Administrator-managed users and roles."),
                        tag("Audit", "Administrator-visible durable audit history.")))
                .components(components);
    }

        @Bean
        OpenApiCustomizer sharedProblemComponents() {
                return openApi -> {
                        Components components = openApi.getComponents();
                        components.addSchemas("ProblemDetail", problemSchema());
                        addProblemResponse(components, "BadRequestProblem", "The request is malformed or fails validation.");
                        addProblemResponse(components, "UnauthorizedProblem", "Authentication is required or credentials are invalid.");
                        addProblemResponse(components, "ForbiddenProblem", "The authenticated identity lacks the required permission.");
                        addProblemResponse(components, "NotFoundProblem", "The requested resource does not exist or is not visible.");
                        addProblemResponse(components, "ConflictProblem", "The request conflicts with the current resource or run state.");
                        addProblemResponse(components, "TooManyRequestsProblem", "The request is throttled after repeated authentication failures.");
                        addProblemResponse(components, "InternalServerErrorProblem", "The server could not complete the request.");
                        applyOperationSecurity(openApi);
                };
        }

        private static void applyOperationSecurity(OpenAPI openApi) {
                Set<String> publicOperations = Set.of("login", "getCsrfToken");
                Set<PathItem.HttpMethod> mutations = Set.of(
                                PathItem.HttpMethod.POST, PathItem.HttpMethod.PUT,
                                PathItem.HttpMethod.PATCH, PathItem.HttpMethod.DELETE);
                openApi.getPaths().values().forEach(path -> path.readOperationsMap().forEach((method, operation) -> {
                        if (publicOperations.contains(operation.getOperationId())) {
                                operation.setSecurity(null);
                                return;
                        }
                        SecurityRequirement requirement = new SecurityRequirement().addList(SESSION_COOKIE_SCHEME);
                        if (mutations.contains(method)) {
                                requirement.addList(CSRF_HEADER_SCHEME);
                        }
                        operation.setSecurity(List.of(requirement));
                }));
        }

        private static Schema<?> problemSchema() {
        return new ObjectSchema()
                .description("RFC 7807 problem response. Dynamic detail text is credential-redacted.")
                .addProperty("type", new StringSchema().format("uri-reference"))
                .addProperty("title", new StringSchema())
                .addProperty("status", new IntegerSchema().format("int32"))
                .addProperty("detail", new StringSchema())
                .addProperty("instance", new StringSchema().format("uri-reference"));
    }

    private static void addProblemResponse(Components components, String name, String description) {
        Schema<?> problemSchema = new ObjectSchema().$ref(PROBLEM_SCHEMA);
        io.swagger.v3.oas.models.media.MediaType problemMediaType =
                new io.swagger.v3.oas.models.media.MediaType().schema(problemSchema);
        components.addResponses(name, new ApiResponse()
                .description(description)
                .content(new Content().addMediaType(MediaType.APPLICATION_PROBLEM_JSON_VALUE, problemMediaType)));
    }

    private static Tag tag(String name, String description) {
        return new Tag().name(name).description(description);
    }
}
