package org.replicadb.server.local;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class PostgresDistributionManifest {

    private final Map<Key, Entry> entries;

    public PostgresDistributionManifest(Collection<Entry> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        Map<Key, Entry> manifest = new HashMap<>();
        for (Entry entry : entries) {
            Key key = new Key(entry.version(), entry.operatingSystem(), entry.architecture());
            if (manifest.put(key, entry) != null) {
                throw new IllegalArgumentException("Duplicate PostgreSQL distribution manifest entry: " + key);
            }
        }
        this.entries = Map.copyOf(manifest);
    }

    public Entry find(String version, String operatingSystem, String architecture) {
        Key key = new Key(version, operatingSystem, architecture);
        Entry entry = entries.get(key);
        if (entry == null) {
            throw new IllegalArgumentException("No PostgreSQL distribution is available for " + key);
        }
        return entry;
    }

    public record Entry(String version, String operatingSystem, String architecture,
                        URI uri, String resourceName, String sha256) {

        public Entry {
            requireSegment(version, "version");
            requireSegment(operatingSystem, "operatingSystem");
            requireSegment(architecture, "architecture");
            Objects.requireNonNull(uri, "uri must not be null");
            if (uri.getUserInfo() != null || (!"http".equalsIgnoreCase(uri.getScheme())
                    && !"https".equalsIgnoreCase(uri.getScheme()))) {
                throw new IllegalArgumentException("uri must use HTTP(S) without user information");
            }
            requireResourceName(resourceName);
            if (sha256 == null || !sha256.matches("[0-9a-fA-F]{64}")) {
                throw new IllegalArgumentException("sha256 must contain 64 hexadecimal characters");
            }
        }

        private static void requireResourceName(String resourceName) {
            if (resourceName == null || resourceName.isBlank()) {
                throw new IllegalArgumentException("resourceName must not be blank");
            }
            Path path = Path.of(resourceName);
            if (path.isAbsolute() || path.getNameCount() != 1 || "..".equals(resourceName)
                    || ".".equals(resourceName)) {
                throw new IllegalArgumentException("resourceName must be a file name");
            }
        }

        private static void requireSegment(String value, String property) {
            if (value == null || value.isBlank() || value.contains("/") || value.contains("\\")
                    || ".".equals(value) || "..".equals(value)) {
                throw new IllegalArgumentException(property + " must be a safe path segment");
            }
        }
    }

    private record Key(String version, String operatingSystem, String architecture) {
        private Key {
            Entry.requireSegment(version, "version");
            Entry.requireSegment(operatingSystem, "operatingSystem");
            Entry.requireSegment(architecture, "architecture");
        }
    }
}
