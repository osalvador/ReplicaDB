package org.replicadb.server.job.api;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobLifecycleIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE run_trigger_idempotency, job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void createsTriggersMonitorsAndCancelsRunsOverHttp() throws Exception {
        Path source = createDatabase("http-source.db", 2);
        Path sink = createDatabase("http-sink.db", 0);
        JobDefinitionResponse definition = createDefinition("http-lifecycle", source, sink);

        JobRunResponse successfulRun = trigger(definition.id(), "http-success-key");
        JobRunResponse completed = awaitStatus(successfulRun.id(), JobRunStatus.SUCCEEDED);
        assertEquals(JobRunStatus.SUCCEEDED, completed.status());
        assertNotNull(completed.finishedAt());

        String jobRuns = restTemplate.getForObject("/api/v1/jobs/" + definition.id() + "/runs", String.class);
        assertTrue(jobRuns.contains(successfulRun.id().toString()));
        String successfulRuns = restTemplate.getForObject("/api/v1/runs?status=SUCCEEDED", String.class);
        assertTrue(successfulRuns.contains(successfulRun.id().toString()));

        Path longSource = createDatabase("http-cancel-source.db", 5000);
        Path longSink = createDatabase("http-cancel-sink.db", 0);
        JobDefinitionResponse cancellableDefinition = createDefinition("http-cancel", longSource, longSink);
        JobRunResponse cancellableRun = trigger(cancellableDefinition.id(), "http-cancel-key");
        awaitStatus(cancellableRun.id(), JobRunStatus.RUNNING);

        HttpHeaders cancelHeaders = new HttpHeaders();
        cancelHeaders.setContentType(MediaType.APPLICATION_JSON);
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

    private JobDefinitionResponse createDefinition(String name, Path source, Path sink) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("sourceConnect", "jdbc:sqlite:" + source);
        body.put("sourceTable", "orders");
        body.put("sinkConnect", "jdbc:sqlite:" + sink);
        body.put("sinkTable", "orders_copy");
        body.put("mode", "complete");
        body.put("jobs", 1);

        ResponseEntity<JobDefinitionResponse> response = restTemplate.postForEntity(
                "/api/v1/jobs", new HttpEntity<>(body, jsonHeaders()), JobDefinitionResponse.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        return response.getBody();
    }

    private JobRunResponse trigger(UUID definitionId, String idempotencyKey) {
        HttpHeaders headers = jsonHeaders();
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
            ResponseEntity<JobRunResponse> response = restTemplate.getForEntity(
                    "/api/v1/runs/" + runId, JobRunResponse.class);
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
