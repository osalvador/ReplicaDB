package org.replicadb.server.local;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresDistributionManagerTest {

    private static final String VERSION = "14.22.0";
    private static final String OPERATING_SYSTEM = "Darwin";
    private static final String ARCHITECTURE = "aarch64";

    @TempDir
    Path temporaryDirectory;

    @Test
    void downloadsOnceAndReusesAValidCacheEntry() throws Exception {
        byte[] payload = "valid-postgres-archive".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = server(payload, requests, null, null);
        try {
            PostgresDistributionManager manager = manager(serverUri(server), payload, 2);

            PostgresDistribution first = manager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE);
            PostgresDistribution second = manager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE);

            assertArrayEquals(payload, Files.readAllBytes(first.archivePath()));
            assertEquals(first.archivePath(), second.archivePath());
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void usesAValidCacheWithoutNetwork() throws Exception {
        byte[] payload = "cached-postgres-archive".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        EmbeddedPostgresHome home = home();
        home.ensureDirectories();
        Path archive = cachePath(home, "postgres.txz");
        Files.createDirectories(archive.getParent());
        Files.write(archive, payload);

        PostgresDistributionManager manager = manager(URI.create("http://127.0.0.1:1/unreachable"), payload, 0);

        PostgresDistribution distribution = manager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE);

        assertEquals(archive, distribution.archivePath());
        assertArrayEquals(payload, Files.readAllBytes(archive));
    }

    @Test
    void rejectsAChangedDownloadAndLeavesNoPartialCache() throws Exception {
        byte[] expectedPayload = "expected-postgres-archive".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] actualPayload = "tampered-postgres-archive".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        HttpServer server = server(actualPayload, new AtomicInteger(), null, null);
        try {
            PostgresDistributionManager manager = manager(serverUri(server), expectedPayload, 3);

            assertThrows(IOException.class,
                    () -> manager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE));

            Path targetDirectory = cachePath(home(), "postgres.txz").getParent();
            assertFalse(Files.exists(targetDirectory.resolve("postgres.txz")));
            try (var files = Files.list(targetDirectory)) {
                assertTrue(files.noneMatch(path -> path.getFileName().toString().endsWith(".part")));
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void retriesNetworkFailuresAndReportsMissingNetwork() {
        PostgresDistributionManager manager = manager(URI.create("http://127.0.0.1:1/unreachable"),
                "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8), 1);

        assertThrows(IOException.class, () -> manager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE));
    }

    @Test
    void rejectsAnUnsupportedPlatformBeforeDownloading() {
        PostgresDistributionManager manager = new PostgresDistributionManager(
                home(), new PostgresDistributionManifest(List.of()), java.time.Duration.ofMillis(100), 0);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> manager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE));

        assertTrue(exception.getMessage().contains("No PostgreSQL distribution"));
    }

    @Test
    void serializesConcurrentAcquisitionOfTheSameDistribution() throws Exception {
        byte[] payload = "concurrent-postgres-archive".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch firstRequestStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstRequest = new CountDownLatch(1);
        ExecutorService httpExecutor = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(httpExecutor);
        server.createContext("/archive",
            exchange -> respond(exchange, payload, requests, firstRequestStarted, releaseFirstRequest));
        server.start();
        ExecutorService acquisitionExecutor = Executors.newFixedThreadPool(2);
        try {
            PostgresDistributionManager firstManager = manager(serverUri(server), payload, 2);
            PostgresDistributionManager secondManager = manager(serverUri(server), payload, 2);
            Future<PostgresDistribution> first = acquisitionExecutor.submit(
                    () -> firstManager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE));
            assertTrue(firstRequestStarted.await(5, TimeUnit.SECONDS));
            Future<PostgresDistribution> second = acquisitionExecutor.submit(
                    () -> secondManager.acquire(VERSION, OPERATING_SYSTEM, ARCHITECTURE));
            releaseFirstRequest.countDown();

            PostgresDistribution firstDistribution = first.get(5, TimeUnit.SECONDS);
            PostgresDistribution secondDistribution = second.get(5, TimeUnit.SECONDS);

            assertEquals(firstDistribution.archivePath(), secondDistribution.archivePath());
            assertEquals(1, requests.get());
        } finally {
            releaseFirstRequest.countDown();
            acquisitionExecutor.shutdownNow();
            httpExecutor.shutdownNow();
            server.stop(0);
        }
    }

    @Test
    void rejectsUnsafeManifestUrisAndResourceNames() {
        assertThrows(IllegalArgumentException.class, () -> new PostgresDistributionManifest(List.of(
                new PostgresDistributionManifest.Entry(VERSION, OPERATING_SYSTEM, ARCHITECTURE,
                    URI.create("https://user:password@example.test/archive"), "postgres.txz",
                    sha256("x".getBytes(java.nio.charset.StandardCharsets.UTF_8))))));
        assertThrows(IllegalArgumentException.class, () -> new PostgresDistributionManifest(List.of(
                new PostgresDistributionManifest.Entry(VERSION, OPERATING_SYSTEM, ARCHITECTURE,
                    URI.create("https://example.test/archive"), "../postgres.txz",
                    sha256("x".getBytes(java.nio.charset.StandardCharsets.UTF_8))))));
    }

    private PostgresDistributionManager manager(URI uri, byte[] payload, int retries) {
        return new PostgresDistributionManager(home(), new PostgresDistributionManifest(List.of(
                new PostgresDistributionManifest.Entry(VERSION, OPERATING_SYSTEM, ARCHITECTURE,
                        uri, "postgres.txz", sha256(payload)))), java.time.Duration.ofMillis(500), retries);
    }

    private EmbeddedPostgresHome home() {
        return EmbeddedPostgresHome.from(temporaryDirectory.resolve("replicadb"));
    }

    private Path cachePath(EmbeddedPostgresHome home, String resourceName) {
        return home.getCacheDirectory().resolve(VERSION)
                .resolve(OPERATING_SYSTEM + "-" + ARCHITECTURE).resolve(resourceName);
    }

    private HttpServer server(byte[] payload, AtomicInteger requests, CountDownLatch started,
                              CountDownLatch release) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/archive", exchange -> respond(exchange, payload, requests, started, release));
        server.start();
        return server;
    }

    private void respond(HttpExchange exchange, byte[] payload, AtomicInteger requests,
                         CountDownLatch started, CountDownLatch release) throws IOException {
        requests.incrementAndGet();
        if (started != null) {
            started.countDown();
        }
        if (release != null) {
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting to release test response");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while serving test response", exception);
            }
        }
        exchange.sendResponseHeaders(200, payload.length);
        try (var output = exchange.getResponseBody()) {
            output.write(payload);
        }
    }

    private URI serverUri(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/archive");
    }

    private static String sha256(byte[] payload) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new AssertionError(exception);
        }
    }
}
