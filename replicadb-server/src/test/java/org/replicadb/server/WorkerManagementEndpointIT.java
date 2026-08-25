package org.replicadb.server;

import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=-1",
                "management.server.port=0",
                "management.server.address=127.0.0.1",
                "management.endpoints.web.exposure.include=health,metrics,prometheus"
        })
@ActiveProfiles("worker")
@Import(PostgresTestcontainersConfig.class)
class WorkerManagementEndpointIT {

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private ServerProperties serverProperties;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Test
    void exposesOnlyTheInternalManagementServer() throws Exception {
        HttpResponse<String> health = get("/actuator/health");
        HttpResponse<String> metrics = get("/actuator/metrics");
        HttpResponse<String> prometheus = get("/actuator/prometheus");

        assertEquals(-1, serverProperties.getPort());
        assertEquals(200, health.statusCode());
        assertEquals(200, metrics.statusCode());
        assertEquals(200, prometheus.statusCode());
        assertTrue(prometheus.body().contains("replicadb_worker"));
        assertTrue(!prometheus.body().contains("leaseToken"));
        assertTrue(!prometheus.body().contains("password"));
    }

    @Test
    void doesNotExposeEnvironmentDetails() throws Exception {
        assertTrue(get("/actuator/env").statusCode() != 200);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return httpClient.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + managementPort + path))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
