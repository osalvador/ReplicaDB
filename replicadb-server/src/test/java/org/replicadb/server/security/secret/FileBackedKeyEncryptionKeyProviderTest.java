package org.replicadb.server.security.secret;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileBackedKeyEncryptionKeyProviderTest {

    @Test
    void loadsCurrentAndPreviousKeysFromAKeyring(@TempDir Path tempDir) throws Exception {
        SecretKey current = key();
        SecretKey previous = key();
        Path keyring = writeKeyring(tempDir, "v2", current, previous);

        FileBackedKeyEncryptionKeyProvider provider = new FileBackedKeyEncryptionKeyProvider(keyring,
                new ObjectMapper());

        assertEquals("v2", provider.current().version());
        assertEquals(32, provider.current().key().getEncoded().length);
        assertEquals(32, provider.find("v1").orElseThrow().key().getEncoded().length);
    }

    @Test
    void rejectsMissingMalformedAndNon256BitKeyrings(@TempDir Path tempDir) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        assertThrows(IllegalStateException.class, () -> new FileBackedKeyEncryptionKeyProvider(
                tempDir.resolve("missing.json"), mapper));

        Path malformed = tempDir.resolve("malformed.json");
        Files.writeString(malformed, "not-json", StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> new FileBackedKeyEncryptionKeyProvider(malformed, mapper));

        Path invalidKey = tempDir.resolve("invalid-key.json");
        Files.writeString(invalidKey, "{\"currentVersion\":\"v1\",\"keys\":{\"v1\":\"AQ==\"}}",
                StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class,
                () -> new FileBackedKeyEncryptionKeyProvider(invalidKey, mapper));
    }

    private static Path writeKeyring(Path tempDir, String currentVersion, SecretKey current,
                                     SecretKey previous) throws Exception {
        String content = "{\"currentVersion\":\"" + currentVersion + "\",\"keys\":{"
                + "\"v2\":\"" + Base64.getEncoder().encodeToString(current.getEncoded()) + "\","
                + "\"v1\":\"" + Base64.getEncoder().encodeToString(previous.getEncoded()) + "\"}}";
        Path path = tempDir.resolve("keyring.json");
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }

    private static SecretKey key() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        return generator.generateKey();
    }
}
