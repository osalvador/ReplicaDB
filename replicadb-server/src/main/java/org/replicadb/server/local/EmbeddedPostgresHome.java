package org.replicadb.server.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

public final class EmbeddedPostgresHome {

    private final Path root;
    private final Path dataDirectory;
    private final Path cacheDirectory;
    private final Path securityDirectory;
    private final Path locksDirectory;
    private final Path runDirectory;
    private final Path logsDirectory;

    private EmbeddedPostgresHome(Path root) {
        this.root = root;
        this.dataDirectory = root.resolve("data").resolve("postgresql");
        this.cacheDirectory = root.resolve("cache").resolve("postgresql");
        this.securityDirectory = root.resolve("security");
        this.locksDirectory = root.resolve("locks");
        this.runDirectory = root.resolve("run");
        this.logsDirectory = root.resolve("logs");
    }

    public static EmbeddedPostgresHome from(Path root) {
        Objects.requireNonNull(root, "root must not be null");
        if (root.toString().isBlank()) {
            throw new IllegalArgumentException("replicadb.server.home must not be blank");
        }
        return new EmbeddedPostgresHome(root.toAbsolutePath().normalize());
    }

    public Path getRoot() {
        return root;
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    public Path getCacheDirectory() {
        return cacheDirectory;
    }

    public Path getSecurityDirectory() {
        return securityDirectory;
    }

    public Path getLocksDirectory() {
        return locksDirectory;
    }

    public Path getRunDirectory() {
        return runDirectory;
    }

    public Path getLogsDirectory() {
        return logsDirectory;
    }

    public Path getKeyringFile() {
        return securityDirectory.resolve("master-key.json");
    }

    public void ensureDirectories() {
        try {
            Files.createDirectories(root);
            Files.createDirectories(dataDirectory);
            Files.createDirectories(cacheDirectory);
            Files.createDirectories(securityDirectory);
            Files.createDirectories(locksDirectory);
            Files.createDirectories(runDirectory);
            Files.createDirectories(logsDirectory);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not prepare ReplicaDB home at " + root, exception);
        }
        try (Stream<Path> directories = Stream.of(root, dataDirectory, cacheDirectory,
            securityDirectory, locksDirectory, runDirectory, logsDirectory)) {
            directories.forEach(this::requireWritableDirectory);
        }
    }

    private void requireWritableDirectory(Path directory) {
        if (!Files.isDirectory(directory) || !Files.isWritable(directory)) {
            throw new IllegalStateException("ReplicaDB directory is not writable: " + directory);
        }
    }
}
