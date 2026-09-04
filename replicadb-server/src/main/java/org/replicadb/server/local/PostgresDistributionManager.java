package org.replicadb.server.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;

public final class PostgresDistributionManager {

    private static final long LOCK_POLL_MILLIS = 50L;
    private static final String USER_AGENT = "ReplicaDB embedded PostgreSQL";

    private final EmbeddedPostgresHome home;
    private final PostgresDistributionManifest manifest;
    private final Duration requestTimeout;
    private final int maxRetries;

    public PostgresDistributionManager(EmbeddedPostgresHome home, PostgresDistributionManifest manifest,
                                       Duration requestTimeout, int maxRetries) {
        this.home = Objects.requireNonNull(home, "home must not be null");
        this.manifest = Objects.requireNonNull(manifest, "manifest must not be null");
        this.requestTimeout = requirePositive(requestTimeout, "requestTimeout");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        this.maxRetries = maxRetries;
    }

    public PostgresDistribution acquire(String version, String operatingSystem, String architecture)
            throws IOException {
        PostgresDistributionManifest.Entry entry = manifest.find(version, operatingSystem, architecture);
        home.ensureDirectories();
        Path targetDirectory = home.getCacheDirectory()
                .resolve(entry.version())
                .resolve(entry.operatingSystem() + "-" + entry.architecture());
        Files.createDirectories(targetDirectory);
        Path archivePath = targetDirectory.resolve(entry.resourceName());
        Path lockPath = home.getLocksDirectory().resolve(
                "postgres-" + entry.version() + "-" + entry.operatingSystem() + "-" + entry.architecture()
                        + ".lock");

        try (FileChannel lockChannel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = acquireLock(lockChannel)) {
            if (!hasExpectedChecksum(archivePath, entry.sha256())) {
                Files.deleteIfExists(archivePath);
                download(entry, archivePath);
            }
            return new PostgresDistribution(entry.version(), entry.operatingSystem(), entry.architecture(),
                    entry.resourceName(), archivePath, entry.sha256().toLowerCase());
        }
    }

    private FileLock acquireLock(FileChannel channel) throws IOException {
        long deadline = System.nanoTime() + requestTimeout.toNanos();
        while (true) {
            try {
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    return lock;
                }
            } catch (OverlappingFileLockException ignored) {
            }
            if (System.nanoTime() >= deadline) {
                throw new IOException("Timed out waiting for the PostgreSQL distribution lock");
            }
            sleepBeforeRetry();
        }
    }

    private void download(PostgresDistributionManifest.Entry entry, Path archivePath) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            Path temporaryPath = Files.createTempFile(archivePath.getParent(), "." + archivePath.getFileName(), ".part");
            try {
                downloadOnce(entry.uri(), temporaryPath);
                verifyChecksum(temporaryPath, entry.sha256());
                moveIntoCache(temporaryPath, archivePath);
                return;
            } catch (NonRetryableDownloadException | IntegrityException exception) {
                lastFailure = exception;
                throw lastFailure;
            } catch (IOException exception) {
                lastFailure = exception;
                if (attempt == maxRetries) {
                    break;
                }
                sleepBeforeRetry();
            } finally {
                Files.deleteIfExists(temporaryPath);
            }
        }
        throw new IOException("Could not acquire PostgreSQL distribution for "
                + entry.operatingSystem() + "/" + entry.architecture(), lastFailure);
    }

    private void downloadOnce(URI uri, Path target) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(timeoutMillis());
        connection.setReadTimeout(timeoutMillis());
        connection.setRequestProperty("User-Agent", USER_AGENT);
        try {
            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                if (status >= 400 && status < 500 && status != 408 && status != 429) {
                    throw new NonRetryableDownloadException("PostgreSQL distribution request was rejected with HTTP "
                            + status);
                }
                throw new IOException("PostgreSQL distribution request failed with HTTP " + status);
            }
            try (InputStream input = connection.getInputStream();
                 OutputStream output = Files.newOutputStream(target, StandardOpenOption.WRITE,
                         StandardOpenOption.TRUNCATE_EXISTING)) {
                input.transferTo(output);
            }
        } finally {
            connection.disconnect();
        }
    }

    private void moveIntoCache(Path temporaryPath, Path archivePath) throws IOException {
        try {
            Files.move(temporaryPath, archivePath, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporaryPath, archivePath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private boolean hasExpectedChecksum(Path archivePath, String expectedChecksum) throws IOException {
        return Files.isRegularFile(archivePath) && expectedChecksum.equalsIgnoreCase(checksum(archivePath));
    }

    private void verifyChecksum(Path archivePath, String expectedChecksum) throws IOException {
        String actualChecksum = checksum(archivePath);
        if (!expectedChecksum.equalsIgnoreCase(actualChecksum)) {
            throw new IntegrityException("PostgreSQL distribution checksum did not match the manifest");
        }
    }

    private String checksum(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private void sleepBeforeRetry() throws IOException {
        try {
            Thread.sleep(LOCK_POLL_MILLIS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acquiring the PostgreSQL distribution", exception);
        }
    }

    private int timeoutMillis() {
        long millis = requestTimeout.toMillis();
        return millis >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) millis;
    }

    private static Duration requirePositive(Duration duration, String property) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(property + " must be positive");
        }
        return duration;
    }

    private static final class NonRetryableDownloadException extends IOException {
        private NonRetryableDownloadException(String message) {
            super(message);
        }
    }

    private static final class IntegrityException extends IOException {
        private IntegrityException(String message) {
            super(message);
        }
    }
}
