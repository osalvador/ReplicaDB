package org.replicadb.server.local;

import java.nio.file.Path;
import java.util.Objects;

public record PostgresDistribution(String version, String operatingSystem, String architecture,
                                   String resourceName, Path archivePath, String sha256) {

    public PostgresDistribution {
        requireText(version, "version");
        requireText(operatingSystem, "operatingSystem");
        requireText(architecture, "architecture");
        requireText(resourceName, "resourceName");
        Objects.requireNonNull(archivePath, "archivePath must not be null");
        requireText(sha256, "sha256");
    }

    private static void requireText(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(property + " must not be blank");
        }
    }
}
