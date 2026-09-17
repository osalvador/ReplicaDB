package org.replicadb.server.local;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.JarURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("embedded-postgres")
class EmbeddedPostgresRuntimeTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void startsOnLoopbackAndStopsItsChildProcess() throws Exception {
        Platform platform = currentPlatform();
        Assumptions.assumeTrue(platform != null, "No embedded PostgreSQL test bundle for this platform");
        EmbeddedPostgresProperties properties = properties();
        byte[] sourceArtifact = sourceArtifact(platform.resourceName());
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = sourceServer(sourceArtifact, requests);
        try {
            EmbeddedPostgresRuntime runtime = new EmbeddedPostgresRuntimeFactory(manifest(server, sourceArtifact, platform))
                    .start(properties);
            Process process = runtime.getProcess();
            try {
                assertTrue(runtime.getPort() > 0);
                assertTrue(runtime.getJdbcUrl().startsWith("jdbc:postgresql://localhost:"));
                assertTrue(Files.isRegularFile(properties.getHome().getDataDirectory().resolve("postmaster.pid")));
                try (Connection connection = runtime.getDataSource().getConnection();
                     Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery("SELECT current_database()")) {
                    assertTrue(resultSet.next());
                    assertEquals("postgres", resultSet.getString(1));
                }
            } finally {
                runtime.close();
                runtime.close();
            }
            assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            assertFalse(process.isAlive());
            assertFalse(Files.exists(properties.getHome().getDataDirectory().resolve("postmaster.pid")));
            assertTrue(Files.isRegularFile(properties.getHome().getDataDirectory().resolve("postgresql.conf")));
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsAnOccupiedConfiguredPort() throws Exception {
        Platform platform = currentPlatform();
        Assumptions.assumeTrue(platform != null, "No embedded PostgreSQL test bundle for this platform");
        byte[] sourceArtifact = sourceArtifact(platform.resourceName());
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = sourceServer(sourceArtifact, requests);
        EmbeddedPostgresRuntime firstRuntime = new EmbeddedPostgresRuntimeFactory(manifest(server, sourceArtifact, platform))
                .start(properties());
        try {
            EmbeddedPostgresProperties secondProperties = properties(firstRuntime.getPort(),
                    temporaryDirectory.resolve("replicadb-second"));
            EmbeddedPostgresRuntime secondRuntime = null;
            try {
                secondRuntime = new EmbeddedPostgresRuntimeFactory(manifest(server, sourceArtifact, platform))
                        .start(secondProperties);
                throw new AssertionError("A second runtime must not use an occupied PostgreSQL port");
            } catch (Exception expected) {
                assertTrue(expected.getMessage() == null || !expected.getMessage().contains("password"));
            } finally {
                if (secondRuntime != null) {
                    secondRuntime.close();
                }
            }
        } finally {
            firstRuntime.close();
            server.stop(0);
        }
    }

    @Test
    void reusesThePersistentClusterAndCachedDistribution() throws Exception {
        Platform platform = currentPlatform();
        Assumptions.assumeTrue(platform != null, "No embedded PostgreSQL test bundle for this platform");
        EmbeddedPostgresProperties properties = properties();
        Path dataDirectory = properties.getHome().getDataDirectory();
        byte[] sourceArtifact = sourceArtifact(platform.resourceName());
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = sourceServer(sourceArtifact, requests);
        try {
            EmbeddedPostgresRuntimeFactory factory = new EmbeddedPostgresRuntimeFactory(
                    manifest(server, sourceArtifact, platform));
            try (EmbeddedPostgresRuntime runtime = factory.start(properties);
                 Connection connection = runtime.getDataSource().getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE persistent_runtime_probe (id INTEGER PRIMARY KEY)");
                statement.execute("INSERT INTO persistent_runtime_probe (id) VALUES (7)");
            }

            try (EmbeddedPostgresRuntime runtime = factory.start(properties);
                 Connection connection = runtime.getDataSource().getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT id FROM persistent_runtime_probe")) {
                assertTrue(resultSet.next());
                assertEquals(7, resultSet.getInt(1));
            }
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
        assertTrue(Files.isRegularFile(dataDirectory.resolve("PG_VERSION")));
        try (var cachedPaths = Files.list(properties.getHome().getCacheDirectory())) {
            assertTrue(cachedPaths.anyMatch(path -> Files.isDirectory(path)));
        }
    }

    private EmbeddedPostgresProperties properties() {
        return properties(0, temporaryDirectory.resolve("replicadb"));
    }

    private EmbeddedPostgresProperties properties(int port) {
        return properties(port, temporaryDirectory.resolve("replicadb"));
    }

    private EmbeddedPostgresProperties properties(int port, Path home) {
        Properties systemProperties = new Properties();
        systemProperties.setProperty(EmbeddedPostgresProperties.ENABLED_PROPERTY, "true");
        systemProperties.setProperty(EmbeddedPostgresProperties.HOME_PROPERTY,
                home.toString());
        systemProperties.setProperty(EmbeddedPostgresProperties.STARTUP_TIMEOUT_PROPERTY, "30s");
        systemProperties.setProperty(EmbeddedPostgresProperties.DOWNLOAD_TIMEOUT_PROPERTY, "30s");
        systemProperties.setProperty(EmbeddedPostgresProperties.DOWNLOAD_RETRIES_PROPERTY, "1");
        systemProperties.setProperty(EmbeddedPostgresProperties.PORT_PROPERTY, Integer.toString(port));
        return EmbeddedPostgresProperties.resolve(systemProperties, Map.of());
    }

    private byte[] sourceArtifact(String resourceName) throws Exception {
        JarURLConnection connection = (JarURLConnection) getClass()
                .getResource("/" + resourceName).openConnection();
        try (JarFile jar = connection.getJarFile()) {
            return Files.readAllBytes(Path.of(jar.getName()));
        }
    }

    private PostgresDistributionManifest manifest(HttpServer server, byte[] sourceArtifact, Platform platform) {
        return new PostgresDistributionManifest(List.of(new PostgresDistributionManifest.Entry(
                "14.22.0", platform.operatingSystem(), platform.architecture(), sourceUri(server),
                platform.resourceName(),
                checksum(sourceArtifact))));
    }

    private HttpServer sourceServer(byte[] sourceArtifact, AtomicInteger requests) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/source", exchange -> respond(exchange, sourceArtifact, requests));
        server.start();
        return server;
    }

    private void respond(HttpExchange exchange, byte[] sourceArtifact, AtomicInteger requests) throws IOException {
        requests.incrementAndGet();
        exchange.sendResponseHeaders(200, sourceArtifact.length);
        try (var output = exchange.getResponseBody()) {
            output.write(sourceArtifact);
        }
    }

    private URI sourceUri(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/source");
    }

    private String checksum(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new AssertionError(exception);
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
