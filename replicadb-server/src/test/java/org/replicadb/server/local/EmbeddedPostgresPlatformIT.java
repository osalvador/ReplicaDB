package org.replicadb.server.local;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.JarURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("embedded-postgres")
class EmbeddedPostgresPlatformIT {

    @Test
    void manifestChecksumsMatchEachBundledPlatformArchive() throws Exception {
        PostgresDistributionManifest manifest = EmbeddedPostgresRuntimeFactory.defaultManifest();
        List<PostgresDistributionManifest.Entry> entries = List.of(
                manifest.find("14.22.0", "Darwin", "aarch64"),
                manifest.find("14.22.0", "Darwin", "x86_64"),
                manifest.find("14.22.0", "Linux", "x86_64"),
                manifest.find("14.22.0", "Windows", "x86_64"));

        for (PostgresDistributionManifest.Entry entry : entries) {
            JarURLConnection connection = (JarURLConnection) getClass()
                    .getResource("/" + entry.resourceName()).openConnection();
            Path archive = Path.of(connection.getJarFileURL().toURI());
            assertNotNull(connection.getJarEntry(), entry.resourceName());
            try (InputStream input = Files.newInputStream(archive)) {
                assertEquals(entry.sha256(), checksum(input), entry.resourceName());
            }
        }
    }

    private String checksum(InputStream input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        input.transferTo(new java.io.OutputStream() {
            @Override
            public void write(int value) {
                digest.update((byte) value);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) {
                digest.update(bytes, offset, length);
            }
        });
        return HexFormat.of().formatHex(digest.digest());
    }
}
