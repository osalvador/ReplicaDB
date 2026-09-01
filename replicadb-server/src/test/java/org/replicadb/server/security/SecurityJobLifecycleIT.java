package org.replicadb.server.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.api.JobDefinitionResponse;
import org.replicadb.server.job.api.JobRunResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class SecurityJobLifecycleIT {

    private static final String ADMIN_USERNAME = "security-admin";
    private static final String ADMIN_PASSWORD = UUID.randomUUID().toString();

    private final HttpClientState admin = new HttpClientState();
    private final HttpClientState operator = new HttpClientState();

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @TempDir
    Path tempDirectory;

    @DynamicPropertySource
    static void securityProperties(DynamicPropertyRegistry registry) {
        registry.add("replicadb.security.bootstrap.enabled", () -> "true");
        registry.add("REPLICADB_BOOTSTRAP_ADMIN_USERNAME", () -> ADMIN_USERNAME);
        registry.add("REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", () -> ADMIN_PASSWORD);
    }

    @BeforeEach
    void clearState() {
        admin.cookies.clear();
        operator.cookies.clear();
        jdbcTemplate.update("TRUNCATE TABLE SPRING_SESSION_ATTRIBUTES, SPRING_SESSION, job_permission, "
            + "run_trigger_idempotency, audit_event, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void enforcesRealLoginRolesAndJobPermissions() throws Exception {
        login(admin, ADMIN_USERNAME, ADMIN_PASSWORD);

        String operatorUsername = "security-operator-" + UUID.randomUUID();
        String operatorPassword = UUID.randomUUID().toString();
        UUID operatorId = createOperator(operatorUsername, operatorPassword);

        Path source = createDatabase("security-source.db", 2);
        Path sink = createDatabase("security-sink.db", 0);
        UUID jobId = createJob(source, sink);
        grantPermissions(jobId, operatorId, "[\"VIEW\",\"EXECUTE\"]");

        login(operator, operatorUsername, operatorPassword);
        get(operator, "/api/v1/jobs/" + jobId, HttpStatus.OK);
        ResponseEntity<String> trigger = post(operator, "/api/v1/jobs/" + jobId + "/runs",
                Map.of(), "security-trigger-" + UUID.randomUUID(), HttpStatus.ACCEPTED);
        JsonNode run = objectMapper.readTree(trigger.getBody());
        UUID runId = UUID.fromString(run.get("id").asText());

        ResponseEntity<String> audit = get(admin, "/api/v1/audit", HttpStatus.OK);
        JsonNode auditContent = objectMapper.readTree(audit.getBody()).get("content");
        assertTrue(containsAction(auditContent, "LOGIN_SUCCEEDED"));
        assertTrue(containsAction(auditContent, "JOB_CREATED"));
        assertTrue(containsAction(auditContent, "RUN_TRIGGERED"));

        post(operator, "/api/v1/runs/" + runId + "/cancel", Map.of(), null, HttpStatus.FORBIDDEN);
        getAnonymousReturnsUnauthorized();
    }

    private UUID createOperator(String username, String password) throws Exception {
        ResponseEntity<String> response = post(admin, "/api/v1/users", Map.of(
                "username", username,
                "password", password,
                "role", "OPERATOR"), null, HttpStatus.CREATED);
        return UUID.fromString(objectMapper.readTree(response.getBody()).get("id").asText());
    }

    private UUID createJob(Path source, Path sink) throws Exception {
        UUID sourceDatasourceId = createDatasource("security-source-datasource-" + UUID.randomUUID(),
            "jdbc:sqlite:" + source);
        UUID sinkDatasourceId = createDatasource("security-sink-datasource-" + UUID.randomUUID(),
            "jdbc:sqlite:" + sink);
        ResponseEntity<String> response = post(admin, "/api/v1/jobs", Map.of(
                "name", "security-job-" + UUID.randomUUID(),
            "sourceDatasourceId", sourceDatasourceId,
                "sourceTable", "orders",
            "sinkDatasourceId", sinkDatasourceId,
                "sinkTable", "orders_copy",
                "mode", "complete",
                "jobs", 1), null, HttpStatus.CREATED);
        return UUID.fromString(objectMapper.readTree(response.getBody()).get("id").asText());
    }

        private UUID createDatasource(String name, String connect) throws Exception {
        ResponseEntity<String> response = post(admin, "/api/v1/datasources", Map.of(
            "name", name,
            "connectorType", "sqlite",
            "technicalParams", Map.of(),
            "security", Map.of("connect", connect),
            "clearSecurityKeys", java.util.List.of()), null, HttpStatus.CREATED);
        return UUID.fromString(objectMapper.readTree(response.getBody()).get("id").asText());
        }

    private void grantPermissions(UUID jobId, UUID userId, String permissions) throws Exception {
        put(admin, "/api/v1/jobs/" + jobId + "/permissions/" + userId,
            Map.of("permissions", objectMapper.readTree(permissions)), HttpStatus.OK);
    }

    private void login(HttpClientState client, String username, String password) throws Exception {
        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/auth/login",
                new HttpEntity<>(Map.of("username", username, "password", password), jsonHeaders()), String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode(), response.getBody());
        client.storeCookies(response);

        ResponseEntity<String> identity = restTemplate.exchange("/api/v1/auth/me", HttpMethod.GET,
                new HttpEntity<>(client.headers(false)), String.class);
        assertEquals(HttpStatus.OK, identity.getStatusCode(), identity.getBody());
        client.storeCookies(identity);
        client.cookies.putIfAbsent("XSRF-TOKEN", UUID.randomUUID().toString());
        assertNotNull(client.cookies.get("SESSION"));
        assertNotNull(client.cookies.get("XSRF-TOKEN"));
    }

    private ResponseEntity<String> get(HttpClientState client, String path, HttpStatus expected) {
        ResponseEntity<String> response = restTemplate.exchange(path, HttpMethod.GET,
                new HttpEntity<>(client.headers(false)), String.class);
        assertEquals(expected, response.getStatusCode(), response.getBody());
        client.storeCookies(response);
        return response;
    }

    private ResponseEntity<String> post(HttpClientState client, String path, Map<String, ?> body,
                                        String idempotencyKey, HttpStatus expected) {
        HttpHeaders headers = client.headers(true);
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        ResponseEntity<String> response = restTemplate.exchange(path, HttpMethod.POST,
                new HttpEntity<>(body, headers), String.class);
        assertEquals(expected, response.getStatusCode(), response.getBody());
        client.storeCookies(response);
        return response;
    }

    private ResponseEntity<String> put(HttpClientState client, String path, Map<String, ?> body,
                                       HttpStatus expected) {
        ResponseEntity<String> response = restTemplate.exchange(path, HttpMethod.PUT,
                new HttpEntity<>(body, client.headers(true)), String.class);
        assertEquals(expected, response.getStatusCode(), response.getBody());
        client.storeCookies(response);
        return response;
    }

    private void getAnonymousReturnsUnauthorized() {
        ResponseEntity<String> response = restTemplate.getForEntity("/api/v1/jobs", String.class);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode(), response.getBody());
    }

    private static boolean containsAction(JsonNode events, String action) {
        for (JsonNode event : events) {
            if (action.equals(event.get("action").asText())) {
                return true;
            }
        }
        return false;
    }

    private static HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    private Path createDatabase(String filename, int rowCount) throws SQLException {
        Path database = tempDirectory.resolve(filename);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, payload TEXT)");
            statement.execute("CREATE TABLE orders_copy (id INTEGER PRIMARY KEY, payload TEXT)");
            for (int index = 1; index <= rowCount; index++) {
                statement.execute("INSERT INTO orders (id, payload) VALUES (" + index
                        + ", 'payload-" + index + "')");
            }
        }
        return database;
    }

    private static final class HttpClientState {

        private final Map<String, String> cookies = new LinkedHashMap<>();

        private HttpHeaders headers(boolean mutating) {
            HttpHeaders headers = jsonHeaders();
            headers.set(HttpHeaders.COOKIE, cookieHeader());
            if (mutating) {
                headers.set("X-XSRF-TOKEN", cookies.get("XSRF-TOKEN"));
            }
            return headers;
        }

        private void storeCookies(ResponseEntity<?> response) {
            for (String value : response.getHeaders().getValuesAsList(HttpHeaders.SET_COOKIE)) {
                String[] cookie = value.split(";", 2)[0].split("=", 2);
                if (cookie.length == 2) {
                    cookies.put(cookie[0], cookie[1]);
                }
            }
        }

        private String cookieHeader() {
            return cookies.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .reduce((first, second) -> first + "; " + second)
                    .orElse("");
        }
    }
}
