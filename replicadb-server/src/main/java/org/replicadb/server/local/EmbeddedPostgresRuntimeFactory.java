package org.replicadb.server.local;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import io.zonky.test.db.postgres.embedded.PgBinaryResolver;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class EmbeddedPostgresRuntimeFactory {

    private static final String POSTGRES_VERSION = "14.22.0";
    private static final String MAVEN_REPOSITORY = "https://repo1.maven.org/maven2/io/zonky/test/postgres/";

    private final PostgresDistributionManifest manifest;

    public EmbeddedPostgresRuntimeFactory() {
        this(defaultManifest());
    }

    EmbeddedPostgresRuntimeFactory(PostgresDistributionManifest manifest) {
        this.manifest = Objects.requireNonNull(manifest, "manifest must not be null");
    }

    public EmbeddedPostgresRuntime start(EmbeddedPostgresProperties properties) throws IOException {
        Objects.requireNonNull(properties, "properties must not be null");
        if (!properties.isEnabled()) {
            throw new IllegalArgumentException("Embedded PostgreSQL is not enabled");
        }
        EmbeddedPostgresHome home = properties.getHome();
        home.ensureDirectories();
        ensurePortAvailable(properties.getPort());
        PostgresDistributionManager distributionManager = new PostgresDistributionManager(home, manifest,
                properties.getDownloadTimeout(), properties.getDownloadRetries());
        PgBinaryResolver binaryResolver = (operatingSystem, architecture) -> {
            PostgresDistribution distribution = distributionManager.acquire(properties.getPostgresVersion(),
                    operatingSystem, architecture);
            return openBinaryResource(distribution);
        };

        EmbeddedPostgres postgres = null;
        try {
            postgres = EmbeddedPostgres.builder()
                    .setDataDirectory(home.getDataDirectory().toFile())
                    .setOverrideWorkingDirectory(home.getCacheDirectory().toFile())
                    .setCleanDataDirectory(false)
                    .setRegisterShutdownHook(false)
                    .setPort(properties.getPort())
                    .setPGStartupWait(properties.getStartupTimeout())
                    .setPgBinaryResolver(binaryResolver)
                    .setServerConfig("timezone", "UTC")
                    .setServerConfig("max_connections", "20")
                    .setServerConfig("shared_buffers", "32MB")
                    .setServerConfig("synchronous_commit", "on")
                    .start();
            if (!Files.isRegularFile(home.getDataDirectory().resolve("postmaster.pid"))) {
                throw new IOException("Embedded PostgreSQL did not create its postmaster pid file");
            }
            return new EmbeddedPostgresRuntime(postgres);
        } catch (IOException | RuntimeException exception) {
            if (postgres != null) {
                closeAfterFailedStart(postgres, exception);
            }
            throw exception;
        }
    }

    private void ensurePortAvailable(int port) throws IOException {
        if (port == 0) {
            return;
        }
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress("127.0.0.1", port));
        } catch (IOException exception) {
            throw new IOException("Configured embedded PostgreSQL port is already in use: " + port, exception);
        }
    }

    private InputStream openBinaryResource(PostgresDistribution distribution) throws IOException {
        JarFile jar = new JarFile(distribution.archivePath().toFile());
        JarEntry entry = jar.getJarEntry(distribution.resourceName());
        if (entry == null) {
            jar.close();
            throw new IOException("PostgreSQL distribution resource is missing from the cache");
        }
        try {
            return new FilterInputStream(jar.getInputStream(entry)) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        jar.close();
                    }
                }
            };
        } catch (IOException exception) {
            jar.close();
            throw exception;
        }
    }

    private void closeAfterFailedStart(EmbeddedPostgres postgres, Exception failure) {
        try {
            postgres.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    static PostgresDistributionManifest defaultManifest() {
        return new PostgresDistributionManifest(List.of(
                entry("Darwin", "aarch64", "embedded-postgres-binaries-darwin-arm64v8",
                        "postgres-darwin-arm_64.txz",
                        "a5a4998de52825a83ec4943837eb6ef87d5e4a96c21ca8ec000ddfe454628f36"),
                entry("Darwin", "x86_64", "embedded-postgres-binaries-darwin-amd64",
                        "postgres-darwin-x86_64.txz",
                        "846e5bb797f372c8e370df51e4151f4e4237058e8c27cfcaf62aa93203a4c5b0"),
                entry("Linux", "x86_64", "embedded-postgres-binaries-linux-amd64",
                        "postgres-linux-x86_64.txz",
                        "c7454ba6125d7c1fd3fa09aefe3a7bca7435c6b7caaf0252d3544b36a6df08ad"),
                entry("Windows", "x86_64", "embedded-postgres-binaries-windows-amd64",
                        "postgres-windows-x86_64.txz",
                        "badc584f9598557e91f4c95d811c277207cd9dac94c7cc5da4ed70dd4e64b307")));
    }

    private static PostgresDistributionManifest.Entry entry(String operatingSystem, String architecture,
                                                              String artifact, String resourceName, String sha256) {
        String fileName = artifact + "-" + POSTGRES_VERSION + ".jar";
        URI uri = URI.create(MAVEN_REPOSITORY + artifact + "/" + POSTGRES_VERSION + "/" + fileName);
        return new PostgresDistributionManifest.Entry(POSTGRES_VERSION, operatingSystem, architecture,
                uri, resourceName, sha256);
    }
}
