package org.replicadb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

class CoreVersionAlignmentTest {

    private static final Pattern CORE_VERSION_PATTERN = Pattern.compile(
            "<artifactId>ReplicaDB</artifactId>\\s*<version>([^<]+)</version>");

    @Test
    void serverDependsOnCurrentRootArtifactVersion() throws IOException {
        String rootPom = Files.readString(Path.of("..", "pom.xml"));
        String serverPom = Files.readString(Path.of("pom.xml"));

        assertEquals(extractCoreVersion(rootPom), extractCoreVersion(serverPom));
    }

    private String extractCoreVersion(String pom) {
        Matcher matcher = CORE_VERSION_PATTERN.matcher(pom);
        assertNotNull(matcher.find() ? matcher : null);
        return matcher.group(1);
    }
}