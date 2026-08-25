package org.replicadb.server;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.replicadb.server.config.PostgresTestcontainersConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
class ManagementConfigurationTest {

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private ServerProperties serverProperties;

        private final HttpClient httpClient = HttpClient.newHttpClient();

    @Test
        void workerHasManagementPortWithoutMainWebServer() throws Exception {
                HttpResponse<String> health = get("/actuator/health");

                assertEquals(200, health.statusCode());
                assertTrue(health.body().contains("status"));
                assertEquals(-1, serverProperties.getPort());
    }

    @Test
        void workerDoesNotExposeEnvironmentEndpoint() throws Exception {
                HttpResponse<String> response = get("/actuator/env");

                assertNotEquals(200, response.statusCode());
        }

        private HttpResponse<String> get(String path) throws Exception {
                HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create("http://localhost:" + managementPort + path))
                                .GET()
                                .build();
                return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
