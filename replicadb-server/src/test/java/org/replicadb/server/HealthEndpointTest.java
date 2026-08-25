package org.replicadb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;
import org.replicadb.server.config.PostgresTestcontainersConfig;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class HealthEndpointTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void healthEndpointReturnsUp() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/actuator/health", String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().contains("\"status\":\"UP\""));
    }

    @Test
    void environmentEndpointIsNotExposed() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/actuator/env", String.class);

        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

        @Test
        void livenessAndReadinessEndpointsArePubliclyProbeable() {
        ResponseEntity<String> liveness = restTemplate.getForEntity(
            "http://localhost:" + port + "/actuator/health/liveness", String.class);
        ResponseEntity<String> readiness = restTemplate.getForEntity(
            "http://localhost:" + port + "/actuator/health/readiness", String.class);

        assertEquals(HttpStatus.OK, liveness.getStatusCode());
        assertEquals(HttpStatus.OK, readiness.getStatusCode());
        assertTrue(liveness.getBody().contains("\"status\":\"UP\""));
        assertTrue(readiness.getBody().contains("\"status\":\"UP\""));
        }

        @Test
        void metricsAndPrometheusEndpointsRequireAuthentication() {
        ResponseEntity<String> metrics = restTemplate.getForEntity(
            "http://localhost:" + port + "/actuator/metrics", String.class);
        ResponseEntity<String> prometheus = restTemplate.getForEntity(
            "http://localhost:" + port + "/actuator/prometheus", String.class);

        assertEquals(HttpStatus.UNAUTHORIZED, metrics.getStatusCode());
        assertEquals(HttpStatus.UNAUTHORIZED, prometheus.getStatusCode());
        }
}
