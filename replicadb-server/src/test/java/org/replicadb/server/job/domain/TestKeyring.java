package org.replicadb.server.job.domain;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;

public final class TestKeyring {

    private TestKeyring() {
    }

    public static Path create() throws Exception {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        Path path = Files.createTempFile("replicadb-test-keyring-", ".json");
        String encoded = Base64.getEncoder().encodeToString(key);
        Files.writeString(path, "{\"currentVersion\":\"test\",\"keys\":{\"test\":\""
                + encoded + "\"}}", StandardCharsets.UTF_8);
        return path;
    }
}
