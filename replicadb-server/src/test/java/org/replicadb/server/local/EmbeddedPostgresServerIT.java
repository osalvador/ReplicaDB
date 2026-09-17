package org.replicadb.server.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.server.ReplicaDbServerApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("embedded-postgres")
class EmbeddedPostgresServerIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String POSTGRES_VERSION = "14.22.0";

    @TempDir
    Path temporaryDirectory;

    @Test
    void startsThroughTheMainLauncherExecutesAJobAndPreservesStateAfterRestart() throws Exception {
        Platform platform = currentPlatform();
        Assumptions.assumeTrue(platform != null, "No embedded PostgreSQL test bundle for this platform");
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(temporaryDirectory.resolve("replicadb"));
        home.ensureDirectories();
        Files.createDirectories(home.getCacheDirectory().resolve(POSTGRES_VERSION)
                .resolve(platform.operatingSystem() + "-" + platform.architecture()));
        Files.write(home.getCacheDirectory().resolve(POSTGRES_VERSION)
                        .resolve(platform.operatingSystem() + "-" + platform.architecture())
                        .resolve(platform.resourceName()), sourceArtifact(platform.resourceName()));

        String suffix = UUID.randomUUID().toString().replace("-", "");
        String username = "embedded-admin-" + suffix;
        String password = UUID.randomUUID().toString();
        String[] arguments = {
                "--replicadb.embedded-postgres.enabled=true",
                "--replicadb.server.home=" + home.getRoot(),
                "--server.port=0",
            "--REPLICADB_BOOTSTRAP_ADMIN_USERNAME=" + username
        };
        Properties systemProperties = new Properties();
        Map<String, String> environment = new HashMap<>(System.getenv());
        environment.put("REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", password);
        ConfigurableApplicationContext firstContext = ReplicaDbServerApplication.launch(
            arguments, systemProperties, environment);
        try {
            int apiPort = apiPort(firstContext);
            HttpClient client = authenticatedClient();
            JdbcTemplate jdbcTemplate = firstContext.getBean(JdbcTemplate.class);
                assertTrue(firstContext.containsBean("adminBootstrapRunner"));
                assertEquals(username, firstContext.getEnvironment()
                    .getProperty("REPLICADB_BOOTSTRAP_ADMIN_USERNAME"));
                assertEquals(password, firstContext.getEnvironment()
                    .getProperty("REPLICADB_BOOTSTRAP_ADMIN_PASSWORD"));
            assertEquals(1, jdbcTemplate.queryForObject("SELECT count(*) FROM app_user", Integer.class));
            assertEquals(username, jdbcTemplate.queryForObject("SELECT username FROM app_user", String.class));
            String csrfToken = authenticate(client, apiPort, username, password);
            int metadataPort = jdbcTemplate.queryForObject("SELECT inet_server_port()", Integer.class);
            String connect = "jdbc:postgresql://localhost:" + metadataPort + "/postgres";
            String sourceTable = "embedded_source_" + suffix;
            String sinkTable = "embedded_sink_" + suffix;
            jdbcTemplate.execute("CREATE TABLE " + sourceTable
                    + " (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)");
            jdbcTemplate.execute("CREATE TABLE " + sinkTable
                    + " (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)");
            jdbcTemplate.update("INSERT INTO " + sourceTable + " (id, payload) VALUES (1, 'one'), (2, 'two')");

            UUID sourceDatasource = createDatasource(client, apiPort, csrfToken, "embedded-source-" + suffix, connect);
            UUID sinkDatasource = createDatasource(client, apiPort, csrfToken, "embedded-sink-" + suffix, connect);
            UUID job = createJob(client, apiPort, csrfToken, "embedded-job-" + suffix,
                    sourceDatasource, sinkDatasource, sourceTable, sinkTable);
            UUID run = triggerRun(client, apiPort, csrfToken, job);
            waitForSuccess(client, apiPort, run);
            assertEquals(2, jdbcTemplate.queryForObject("SELECT count(*) FROM " + sinkTable, Integer.class));
        } finally {
            firstContext.close();
        }

        assertTrue(Files.isRegularFile(home.getKeyringFile()));
        ConfigurableApplicationContext secondContext = ReplicaDbServerApplication.launch(
                arguments, systemProperties, Map.of());
        try {
            int apiPort = apiPort(secondContext);
            HttpClient client = authenticatedClient();
            authenticate(client, apiPort, username, password);
            HttpResponse<String> jobs = request(client, apiPort, "GET", "/api/v1/jobs?page=0&size=10", null, null);
            assertEquals(200, jobs.statusCode());
            JsonNode body = OBJECT_MAPPER.readTree(jobs.body());
            assertTrue(body.path("totalElements").asInt() >= 1);
        } finally {
            secondContext.close();
        }
    }

    private HttpClient authenticatedClient() {
        CookieManager cookieManager = new CookieManager(null, CookiePolicy.ACCEPT_ALL);
        return HttpClient.newBuilder().cookieHandler(cookieManager).build();
    }

    private String csrfToken(HttpClient client, int apiPort) throws Exception {
        HttpResponse<String> response = request(client, apiPort, "GET", "/api/v1/auth/csrf", null, null);
        assertEquals(200, response.statusCode());
        return OBJECT_MAPPER.readTree(response.body()).path("token").asText();
    }

    private String authenticate(HttpClient client, int apiPort, String username, String password) throws Exception {
        String csrfToken = csrfToken(client, apiPort);
        String body = "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}";
        HttpResponse<String> response = request(client, apiPort, "POST", "/api/v1/auth/login", body, csrfToken);
        assertEquals(200, response.statusCode());
        return csrfToken;
    }

    private UUID createDatasource(HttpClient client, int apiPort, String csrfToken,
                                  String name, String connect) throws Exception {
        String body = "{\"name\":\"" + name + "\",\"connectorType\":\"postgres\","
                + "\"technicalParams\":{},\"security\":{\"connect\":\"" + connect
                + "\",\"user\":\"postgres\"},\"clearSecurityKeys\":[]}";
        HttpResponse<String> response = request(client, apiPort, "POST", "/api/v1/datasources", body, csrfToken);
        assertEquals(201, response.statusCode());
        return UUID.fromString(OBJECT_MAPPER.readTree(response.body()).path("id").asText());
    }

    private UUID createJob(HttpClient client, int apiPort, String csrfToken, String name,
                           UUID sourceDatasource, UUID sinkDatasource, String sourceTable,
                           String sinkTable) throws Exception {
        String body = "{\"name\":\"" + name + "\","
                + "\"sourceDatasourceId\":\"" + sourceDatasource + "\","
                + "\"sourceDatasourceUseEnabled\":true,"
                + "\"sourceTable\":\"" + sourceTable + "\","
                + "\"sourceColumns\":\"id, payload\","
                + "\"sinkDatasourceId\":\"" + sinkDatasource + "\","
                + "\"sinkDatasourceUseEnabled\":true,"
                + "\"sinkTable\":\"" + sinkTable + "\","
                + "\"sinkColumns\":\"id, payload\","
                + "\"mode\":\"complete\",\"jobs\":1,"
                + "\"fetchSize\":100,\"bandwidthThrottling\":0,\"verbose\":false}";
        HttpResponse<String> response = request(client, apiPort, "POST", "/api/v1/jobs", body, csrfToken);
        assertEquals(201, response.statusCode());
        return UUID.fromString(OBJECT_MAPPER.readTree(response.body()).path("id").asText());
    }

    private UUID triggerRun(HttpClient client, int apiPort, String csrfToken, UUID job) throws Exception {
        HttpResponse<String> response = request(client, apiPort, "POST", "/api/v1/jobs/" + job + "/runs", null, csrfToken);
        assertEquals(202, response.statusCode());
        return UUID.fromString(OBJECT_MAPPER.readTree(response.body()).path("id").asText());
    }

    private void waitForSuccess(HttpClient client, int apiPort, UUID run) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            HttpResponse<String> response = request(client, apiPort, "GET", "/api/v1/runs/" + run, null, null);
            assertEquals(200, response.statusCode());
            String status = OBJECT_MAPPER.readTree(response.body()).path("status").asText();
            if ("SUCCEEDED".equals(status)) {
                return;
            }
            assertTrue("PENDING".equals(status) || "RUNNING".equals(status),
                    "Unexpected run status: " + status);
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new AssertionError("Embedded run did not succeed within the timeout");
    }

    private HttpResponse<String> request(HttpClient client, int apiPort, String method, String path,
                                         String body, String csrfToken) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + apiPort + path));
        if (csrfToken != null) {
            builder.header("X-XSRF-TOKEN", csrfToken);
        }
        if ("POST".equals(method)) {
            builder.header("Content-Type", "application/json");
            if (body == null) {
                builder.header("Idempotency-Key", "embedded-test-" + UUID.randomUUID());
                builder.POST(HttpRequest.BodyPublishers.noBody());
            } else {
                builder.POST(HttpRequest.BodyPublishers.ofString(body));
            }
        } else {
            builder.GET();
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private int apiPort(ConfigurableApplicationContext context) {
        return ((org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext) context)
                .getWebServer().getPort();
    }

    private byte[] sourceArtifact(String resourceName) throws Exception {
        JarURLConnection connection = (JarURLConnection) getClass().getResource("/" + resourceName).openConnection();
        try (JarFile jar = connection.getJarFile()) {
            return Files.readAllBytes(Path.of(jar.getName()));
        }
    }

    private Platform currentPlatform() {
        String operatingSystem = System.getProperty("os.name");
        String architecture = "amd64".equals(System.getProperty("os.arch"))
                ? "x86_64" : System.getProperty("os.arch");
        if (operatingSystem != null && operatingSystem.startsWith("Mac OS X")) {
            if ("aarch64".equals(architecture)) {
                return new Platform("Darwin", architecture, "postgres-darwin-arm_64.txz");
            }
            if ("x86_64".equals(architecture)) {
                return new Platform("Darwin", architecture, "postgres-darwin-x86_64.txz");
            }
        }
        if (operatingSystem != null && operatingSystem.startsWith("Linux") && "x86_64".equals(architecture)) {
            return new Platform("Linux", architecture, "postgres-linux-x86_64.txz");
        }
        if (operatingSystem != null && operatingSystem.startsWith("Windows") && "x86_64".equals(architecture)) {
            return new Platform("Windows", architecture, "postgres-windows-x86_64.txz");
        }
        return null;
    }

    private record Platform(String operatingSystem, String architecture, String resourceName) {
    }
}
