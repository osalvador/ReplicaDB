package org.replicadb.server.security.secret;

import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvBackedKeyEncryptionKeyProviderTest {

    @Test
    void loadsCurrentKeyWithoutSecondaryKey() throws Exception {
        String current = encodedKey();

        EnvBackedKeyEncryptionKeyProvider provider = new EnvBackedKeyEncryptionKeyProvider(
                "v1", current, "", "");

        assertEquals("v1", provider.current().version());
        assertEquals(Set.of("v1"), provider.knownVersions());
    }

    @Test
    void loadsCurrentAndSecondaryKeys() throws Exception {
        EnvBackedKeyEncryptionKeyProvider provider = new EnvBackedKeyEncryptionKeyProvider(
                "v2", encodedKey(), "v1", encodedKey());

        assertEquals("v2", provider.current().version());
        assertEquals(Set.of("v1", "v2"), provider.knownVersions());
        assertEquals("v1", provider.find("v1").orElseThrow().version());
    }

    @Test
    void rejectsBlankCurrentVersion() throws Exception {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new EnvBackedKeyEncryptionKeyProvider(" ", encodedKey(), "", ""));

        assertTrue(exception.getMessage().contains("CURRENT_VERSION"));
    }

    @Test
    void rejectsMalformedCurrentKey() {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new EnvBackedKeyEncryptionKeyProvider("v1", "not-valid-base64!!", "", ""));

        assertTrue(exception.getMessage().contains("REPLICADB_SECURITY_KEYRING_CURRENT_KEY"));
        assertTrue(exception.getMessage().contains("not valid Base64"));
    }

    @Test
    void rejectsCurrentKeyWithWrongDecodedLength() {
        String oneByte = Base64.getEncoder().encodeToString(new byte[31]);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new EnvBackedKeyEncryptionKeyProvider("v1", oneByte, "", ""));

        assertTrue(exception.getMessage().contains("REPLICADB_SECURITY_KEYRING_CURRENT_KEY"));
        assertTrue(exception.getMessage().contains("31"));
    }

    @Test
    void acceptsSurroundingWhitespaceInCurrentKey() throws Exception {
        String current = encodedKey();

        EnvBackedKeyEncryptionKeyProvider provider = new EnvBackedKeyEncryptionKeyProvider(
                "v1", " \t" + current + " \n", "", "");

        assertEquals("v1", provider.current().version());
    }

    @Test
    void rejectsIncompleteSecondarySlot() throws Exception {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new EnvBackedKeyEncryptionKeyProvider("v1", encodedKey(), "v2", ""));

        assertTrue(exception.getMessage().contains("SECONDARY"));
    }

    @Test
    void rejectsDuplicateCurrentAndSecondaryVersions() throws Exception {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new EnvBackedKeyEncryptionKeyProvider("v1", encodedKey(), "v1", encodedKey()));

        assertTrue(exception.getMessage().contains("must differ"));
    }

    private static String encodedKey() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        SecretKey key = generator.generateKey();
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }
}
