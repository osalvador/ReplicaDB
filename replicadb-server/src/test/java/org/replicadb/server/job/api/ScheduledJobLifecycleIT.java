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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class ScheduledJobLifecycleIT {

    private static final String BOOTSTRAP_USERNAME = "scheduled-admin";
    private static final String BOOTSTRAP_PASSWORD = UUID.randomUUID().toString();

    private final Map<String, String> cookies = new LinkedHashMap<>();

    @DynamicPropertySource
    static void securityProperties(DynamicPropertyRegistry registry) {
        registry.add("replicadb.security.bootstrap.enabled", () -> "true");
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
        jdbcTemplate.update("TRUNCATE TABLE SPRING_SESSION_ATTRIBUTES, SPRING_SESSION, job_schedule, run_trigger_idempotency, job_run, job_definition CASCADE",
                Map.of());
    }

    @Test
    void firesAScheduleAndRejectsAnOverlappingManualRun() throws Exception {
        login();
        Path source = createDatabase("scheduled-source.db", 50_000);
        Path sink = createDatabase("scheduled-sink.db", 0);
        JobDefinitionResponse definition = createDefinition("scheduled-lifecycle", source, sink);

        ResponseEntity<String> schedule = restTemplate.exchange(
                "/api/v1/jobs/" + definition.id() + "/schedule",
                HttpMethod.PUT,
                new HttpEntity<>(Map.of(
                        "cronExpression", "*/1 * * * * ?",
                        "timeZone", "UTC",
                        "enabled", true), authenticatedHeaders(true)),
                String.class);
        assertEquals(HttpStatus.OK, schedule.getStatusCode());
        assertNotNull(schedule.getBody());

        boolean overlapRejected = false;
        boolean succeeded = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline && !succeeded) {
            ResponseEntity<String> runsResponse = restTemplate.exchange(
                "/api/v1/jobs/" + definition.id() + "/runs", HttpMethod.GET,
                new HttpEntity<>(authenticatedHeaders(false)), String.class);
            assertEquals(HttpStatus.OK, runsResponse.getStatusCode());
            JsonNode runs = objectMapper.readTree(runsResponse.getBody());
            for (JsonNode run : runs.path("content")) {
                JobRunStatus status = JobRunStatus.valueOf(run.path("status").asText());
                if (!overlapRejected && (status == JobRunStatus.PENDING || status == JobRunStatus.RUNNING)) {
                    ResponseEntity<String> manual = triggerManually(definition.id());
                    assertEquals(HttpStatus.CONFLICT, manual.getStatusCode());
                    overlapRejected = true;
                }
                if (status == JobRunStatus.SUCCEEDED) {
                    succeeded = true;
                    break;
                }
                if (status == JobRunStatus.FAILED || status == JobRunStatus.CANCELLED) {
                    throw new AssertionError("Scheduled run reached unexpected status " + status);
                }
            }
            if (!succeeded) {
                Thread.sleep(25);
            }
        }

        assertTrue(succeeded, "A scheduled run did not reach SUCCEEDED within the timeout");
        assertTrue(overlapRejected, "A manual run was not observed while the scheduled run was active");
    }

    private ResponseEntity<String> triggerManually(UUID jobDefinitionId) {
        HttpHeaders headers = authenticatedHeaders(true);
        headers.set("Idempotency-Key", "scheduled-overlap-" + UUID.randomUUID());
        return restTemplate.exchange(
                "/api/v1/jobs/" + jobDefinitionId + "/runs",
                HttpMethod.POST,
                new HttpEntity<>(headers),
                String.class);
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
        assertNotNull(response.getBody());
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

    private Path createDatabase(String filename, int rowCount) throws SQLException {
        Path database = tempDirectory.resolve(filename);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, payload TEXT)");
            statement.execute("CREATE TABLE orders_copy (id INTEGER PRIMARY KEY, payload TEXT)");
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO orders (id, payload) VALUES (?, ?)")) {
                for (int index = 1; index <= rowCount; index++) {
                    insert.setInt(1, index);
                    insert.setString(2, "payload-" + index);
                    insert.addBatch();
                    if (index % 1_000 == 0) {
                        insert.executeBatch();
                    }
                }
                insert.executeBatch();
            }
            connection.commit();
        }
        return database;
    }

    private static HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }
}
