package org.replicadb.server.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobRunStatus;
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
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobLifecycleIT {

    private static final String BOOTSTRAP_USERNAME = "lifecycle-admin";
    private static final String BOOTSTRAP_PASSWORD = UUID.randomUUID().toString();

    private final Map<String, String> cookies = new LinkedHashMap<>();

    @DynamicPropertySource
    static void securityProperties(DynamicPropertyRegistry registry) {
        registry.add("replicadb.security.bootstrap.enabled", () -> "true");
        registry.add("replicadb.server.local-seeding.enabled", () -> "true");
        registry.add("REPLICADB_BOOTSTRAP_ADMIN_USERNAME", () -> BOOTSTRAP_USERNAME);
        registry.add("REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", () -> BOOTSTRAP_PASSWORD);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        cookies.clear();
        jdbcTemplate.update("TRUNCATE TABLE SPRING_SESSION_ATTRIBUTES, SPRING_SESSION, run_trigger_idempotency, job_run, job_definition CASCADE",
                Map.of());
    }

    @Test
    void createsTriggersMonitorsAndCancelsRunsOverHttp() throws Exception {
        login();
        Path source = createDatabase("http-source.db", 2);
        Path sink = createDatabase("http-sink.db", 0);
        JobDefinitionResponse definition = createDefinition("http-lifecycle", source, sink);

        JobRunResponse successfulRun = trigger(definition.id(), "http-success-key");
        JobRunResponse completed = awaitStatus(successfulRun.id(), JobRunStatus.SUCCEEDED);
        assertEquals(JobRunStatus.SUCCEEDED, completed.status());
        assertNotNull(completed.finishedAt());

        String jobRuns = getAuthenticated("/api/v1/jobs/" + definition.id() + "/runs");
        assertTrue(jobRuns.contains(successfulRun.id().toString()));
        String successfulRuns = getAuthenticated("/api/v1/runs?status=SUCCEEDED");
        assertTrue(successfulRuns.contains(successfulRun.id().toString()));

        Path longSource = createDatabase("http-cancel-source.db", 5000);
        Path longSink = createDatabase("http-cancel-sink.db", 0);
        JobDefinitionResponse cancellableDefinition = createDefinition("http-cancel", longSource, longSink);
        JobRunResponse cancellableRun = trigger(cancellableDefinition.id(), "http-cancel-key");
        awaitStatus(cancellableRun.id(), JobRunStatus.RUNNING);

        HttpHeaders cancelHeaders = authenticatedHeaders(true);
        ResponseEntity<JobRunController.CancellationResponse> cancellation = restTemplate.exchange(
                "/api/v1/runs/" + cancellableRun.id() + "/cancel",
                HttpMethod.POST,
                new HttpEntity<>(cancelHeaders),
                JobRunController.CancellationResponse.class);

        assertEquals(HttpStatus.OK, cancellation.getStatusCode());
        assertNotNull(cancellation.getBody());
        assertTrue(cancellation.getBody().warning() != null && !cancellation.getBody().warning().isBlank());
        JobRunResponse cancelled = awaitStatus(cancellableRun.id(), JobRunStatus.CANCELLED);
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
    }

    @Test
    void seedsTerminalHistoryWithoutStartingCoreExecution() throws Exception {
        login();
        Path source = createDatabase("http-local-seed-source.db", 0);
        Path sink = createDatabase("http-local-seed-sink.db", 0);
        JobDefinitionResponse definition = createDefinition("http-local-seed", source, sink);

        for (int index = 0; index < 5; index += 1) {
            HttpHeaders headers = authenticatedHeaders(true);
            headers.set("Idempotency-Key", "http-local-seed-" + index);
            headers.set("X-ReplicaDB-Local-Seed", "true");

            ResponseEntity<JobRunResponse> response = restTemplate.postForEntity(
                "/api/v1/jobs/" + definition.id() + "/runs",
                new HttpEntity<>(headers), JobRunResponse.class);

            assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
            assertNotNull(response.getBody());
            assertEquals(JobRunStatus.CANCELLED, response.getBody().status());
            assertNull(response.getBody().startedAt());
            assertNull(response.getBody().executorIdentity());
            assertNotNull(response.getBody().finishedAt());
        }

        JsonNode history = objectMapper.readTree(
            getAuthenticated("/api/v1/jobs/" + definition.id() + "/runs?size=100"));
        assertEquals(5, history.get("totalElements").asInt());
    }

        @Test
        void supportsQueryOnlySourcesAndRejectsCredentialLikeParameters() throws Exception {
        login();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "query-only-lifecycle");
        UUID sourceDatasourceId = createDatasource("query-source-lifecycle",
            "jdbc:sqlite:" + tempDirectory.resolve("query-source.db"));
        UUID sinkDatasourceId = createDatasource("query-sink-lifecycle",
            "jdbc:sqlite:" + tempDirectory.resolve("query-sink.db"));
        body.put("sourceDatasourceId", sourceDatasourceId);
        body.put("sourceQuery", "select id from source_table");
        body.put("sinkDatasourceId", sinkDatasourceId);
        body.put("sinkTable", "sink_table");
        body.put("mode", "complete");
        body.put("jobs", 1);

        ResponseEntity<String> created = restTemplate.postForEntity(
            "/api/v1/jobs", new HttpEntity<>(body, authenticatedHeaders(true)), String.class);
        assertEquals(HttpStatus.CREATED, created.getStatusCode(), created.getBody());
        JobDefinitionResponse createdDefinition = objectMapper.readValue(created.getBody(), JobDefinitionResponse.class);
        assertEquals("select id from source_table", createdDefinition.sourceQuery());
        assertEquals(null, createdDefinition.sourceTable());

        body.remove("name");
        body.put("sourceQuery", "select id from source_table where id > 10");
        ResponseEntity<String> updated = restTemplate.exchange(
            "/api/v1/jobs/" + createdDefinition.id(), HttpMethod.PUT,
            new HttpEntity<>(body, authenticatedHeaders(true)), String.class);
        assertEquals(HttpStatus.OK, updated.getStatusCode(), updated.getBody());
        JobDefinitionResponse updatedDefinition = objectMapper.readValue(updated.getBody(), JobDefinitionResponse.class);
        assertEquals("select id from source_table where id > 10", updatedDefinition.sourceQuery());

        Map<String, Object> credentialLike = new LinkedHashMap<>(body);
        credentialLike.put("name", "credential-like-parameter");
        credentialLike.put("sourceConnectionParams", Map.of("password", "not-a-secret"));
        ResponseEntity<String> rejectedParams = restTemplate.postForEntity(
            "/api/v1/jobs", new HttpEntity<>(credentialLike, authenticatedHeaders(true)), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, rejectedParams.getStatusCode());

        Map<String, Object> missingSource = new LinkedHashMap<>(body);
        missingSource.put("name", "missing-source-selection");
        missingSource.remove("sourceQuery");
        ResponseEntity<String> rejectedSource = restTemplate.postForEntity(
            "/api/v1/jobs", new HttpEntity<>(missingSource, authenticatedHeaders(true)), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, rejectedSource.getStatusCode());
        assertTrue(rejectedSource.getBody() != null
            && rejectedSource.getBody().contains("source table or query must be configured"));
        }

    private JobDefinitionResponse createDefinition(String name, Path source, Path sink) {
        UUID sourceDatasourceId = createDatasource(name + "-source-datasource", "jdbc:sqlite:" + source);
        UUID sinkDatasourceId = createDatasource(name + "-sink-datasource", "jdbc:sqlite:" + sink);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("sourceDatasourceId", sourceDatasourceId);
        body.put("sourceTable", "orders");
        body.put("sinkDatasourceId", sinkDatasourceId);
        body.put("sinkTable", "orders_copy");
        body.put("mode", "complete");
        body.put("jobs", 1);

        ResponseEntity<String> response = restTemplate.postForEntity(
            "/api/v1/jobs", new HttpEntity<>(body, authenticatedHeaders(true)), String.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode(), response.getBody());
        try {
            return objectMapper.readValue(response.getBody(), JobDefinitionResponse.class);
        } catch (Exception exception) {
            throw new AssertionError("Could not parse job definition response", exception);
        }
    }

    private UUID createDatasource(String name, String connect) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("connectorType", "sqlite");
        body.put("technicalParams", Map.of());
        body.put("security", Map.of("connect", connect));
        body.put("clearSecurityKeys", java.util.List.of());
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/datasources", new HttpEntity<>(body, authenticatedHeaders(true)), String.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode(), response.getBody());
        try {
            return UUID.fromString(objectMapper.readTree(response.getBody()).get("id").asText());
        } catch (Exception exception) {
            throw new AssertionError("Could not parse datasource response", exception);
        }
    }

    private JobRunResponse trigger(UUID definitionId, String idempotencyKey) {
        HttpHeaders headers = authenticatedHeaders(true);
        headers.set("Idempotency-Key", idempotencyKey);
        ResponseEntity<JobRunResponse> response = restTemplate.postForEntity(
            "/api/v1/jobs/" + definitionId + "/runs", new HttpEntity<>(headers), JobRunResponse.class);
        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    private JobRunResponse awaitStatus(UUID runId, JobRunStatus expectedStatus) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            ResponseEntity<JobRunResponse> response = restTemplate.exchange(
                "/api/v1/runs/" + runId, HttpMethod.GET,
                new HttpEntity<>(authenticatedHeaders(false)), JobRunResponse.class);
            JobRunResponse run = response.getBody();
            if (run != null && run.status() == expectedStatus) {
                return run;
            }
            if (run != null && run.status().isTerminal() && run.status() != expectedStatus) {
                throw new AssertionError("Run reached unexpected status " + run.status());
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Run did not reach " + expectedStatus + ": " + runId);
    }

    private void login() {
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/auth/login",
                new HttpEntity<>(Map.of("username", BOOTSTRAP_USERNAME, "password", BOOTSTRAP_PASSWORD), jsonHeaders()),
                String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        storeCookies(response);
        ResponseEntity<String> identity = restTemplate.exchange("/api/v1/auth/me", HttpMethod.GET,
            new HttpEntity<>(authenticatedHeaders(false)), String.class);
        assertEquals(HttpStatus.OK, identity.getStatusCode());
        storeCookies(identity);
        cookies.putIfAbsent("XSRF-TOKEN", UUID.randomUUID().toString());
    }

    private String getAuthenticated(String path) {
        ResponseEntity<String> response = restTemplate.exchange(path, HttpMethod.GET,
                new HttpEntity<>(authenticatedHeaders(false)), String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        storeCookies(response);
        return response.getBody();
    }

    private HttpHeaders authenticatedHeaders(boolean mutating) {
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
                statement.execute("INSERT INTO orders (id, payload) VALUES (" + index + ", 'payload-" + index + "')");
            }
        }
        return database;
    }
}
