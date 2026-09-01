package org.replicadb.server.security.secret;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SecretProtectionServiceTest {

    @Test
    void encryptsAndDecryptsCanonicalSecurityValues() throws Exception {
        TestKeyProvider provider = new TestKeyProvider();
        SecretProtectionService service = new SecretProtectionService(provider, new SecureRandom(), new ObjectMapper());
        UUID datasourceId = UUID.randomUUID();

        EncryptedSecurityBundle bundle = service.encrypt(datasourceId,
                Map.of("password", "placeholder-password", "connect", "mongodb://user:placeholder@host/db"));

        assertEquals(Map.of("password", "placeholder-password", "connect", "mongodb://user:placeholder@host/db"),
                service.decrypt(datasourceId, bundle));
        assertEquals("v1", bundle.keyVersion());
        assertEquals(EncryptedSecurityBundle.AES_256_GCM_ALGORITHM, bundle.algorithm());
    }

    @Test
    void generatesFreshNonceAndDataKeyForEachBundle() throws Exception {
        SecretProtectionService service = new SecretProtectionService(new TestKeyProvider(),
                new SecureRandom(), new ObjectMapper());
        UUID datasourceId = UUID.randomUUID();

        EncryptedSecurityBundle first = service.encrypt(datasourceId, Map.of("password", "one"));
        EncryptedSecurityBundle second = service.encrypt(datasourceId, Map.of("password", "one"));

        assertNotEquals(java.util.Arrays.toString(first.nonce()), java.util.Arrays.toString(second.nonce()));
        assertNotEquals(java.util.Arrays.toString(first.wrappedDataKey()),
                java.util.Arrays.toString(second.wrappedDataKey()));
        assertNotEquals(java.util.Arrays.toString(first.ciphertext()),
                java.util.Arrays.toString(second.ciphertext()));
    }

    @Test
    void rejectsTamperingAndWrongDatasource() throws Exception {
        SecretProtectionService service = new SecretProtectionService(new TestKeyProvider(),
                new SecureRandom(), new ObjectMapper());
        UUID datasourceId = UUID.randomUUID();
        EncryptedSecurityBundle bundle = service.encrypt(datasourceId, Map.of("password", "placeholder"));
        byte[] tampered = bundle.ciphertext();
        tampered[0] ^= 1;
        EncryptedSecurityBundle changed = new EncryptedSecurityBundle(bundle.formatVersion(), bundle.algorithm(),
                bundle.keyVersion(), bundle.wrappedDataKey(), bundle.nonce(), tampered);

        assertThrows(IllegalArgumentException.class, () -> service.decrypt(datasourceId, changed));
        assertThrows(IllegalArgumentException.class, () -> service.decrypt(UUID.randomUUID(), bundle));
    }

    @Test
    void reencryptsUsingTheCurrentKeyVersion() throws Exception {
        TestKeyProvider provider = new TestKeyProvider();
        SecretProtectionService service = new SecretProtectionService(provider, new SecureRandom(), new ObjectMapper());
        UUID datasourceId = UUID.randomUUID();
        EncryptedSecurityBundle original = service.encrypt(datasourceId, Map.of("password", "placeholder"));

        provider.rotate();
        EncryptedSecurityBundle rotated = service.reencrypt(datasourceId, original);

        assertEquals("v2", rotated.keyVersion());
        assertEquals(Map.of("password", "placeholder"), service.decrypt(datasourceId, rotated));
        assertEquals(Map.of("password", "placeholder"), service.decrypt(datasourceId, original));
    }

    @Test
    void protectsMutableBundleArrays() throws Exception {
        SecretProtectionService service = new SecretProtectionService(new TestKeyProvider(),
                new SecureRandom(), new ObjectMapper());
        EncryptedSecurityBundle bundle = service.encrypt(UUID.randomUUID(), Map.of("password", "placeholder"));
        byte[] ciphertext = bundle.ciphertext();
        ciphertext[0] ^= 1;

        assertNotEquals(java.util.Arrays.toString(ciphertext),
            java.util.Arrays.toString(bundle.ciphertext()));
    }

    @Test
    void serializesAndDeserializesEncryptedBundles() throws Exception {
        SecretProtectionService service = new SecretProtectionService(new TestKeyProvider(),
                new SecureRandom(), new ObjectMapper());
        UUID datasourceId = UUID.randomUUID();
        EncryptedSecurityBundle original = service.encrypt(datasourceId,
                Map.of("connect", "jdbc:source", "password", "placeholder"));

        EncryptedSecurityBundle restored = service.deserialize(service.serialize(original));

        assertEquals(original.formatVersion(), restored.formatVersion());
        assertEquals(original.algorithm(), restored.algorithm());
        assertEquals(original.keyVersion(), restored.keyVersion());
        assertEquals(service.decrypt(datasourceId, original), service.decrypt(datasourceId, restored));
    }

    @Test
    void rejectsAnEmptySerializedBundle() {
        SecretProtectionService service = new SecretProtectionService(new TestKeyProvider(),
                new SecureRandom(), new ObjectMapper());

        assertThrows(IllegalArgumentException.class, () -> service.deserialize(new byte[0]));
    }

    private static final class TestKeyProvider implements KeyEncryptionKeyProvider {
        private final KeyEncryptionKey first = key("v1");
        private final KeyEncryptionKey second = key("v2");
        private boolean rotated;

        @Override
        public KeyEncryptionKey current() {
            return rotated ? second : first;
        }

        @Override
        public Optional<KeyEncryptionKey> find(String version) {
            if (first.version().equals(version)) {
                return Optional.of(first);
            }
            if (second.version().equals(version)) {
                return Optional.of(second);
            }
            return Optional.empty();
        }

        void rotate() {
            rotated = true;
        }

        private static KeyEncryptionKey key(String version) {
            try {
                KeyGenerator generator = KeyGenerator.getInstance("AES");
                generator.init(256);
                SecretKey key = generator.generateKey();
                return new KeyEncryptionKey(version, key);
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        }
    }
}
