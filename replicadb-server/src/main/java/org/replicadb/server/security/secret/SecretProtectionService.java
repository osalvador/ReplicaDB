package org.replicadb.server.security.secret;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public final class SecretProtectionService {

    private static final int DATA_KEY_LENGTH = 32;
    private static final int NONCE_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;
    private static final String KEY_WRAP_ALGORITHM = "AESWrap";
    private static final String ASSOCIATED_DATA_PREFIX = "replicadb.datasource.security.v1:";

    private final KeyEncryptionKeyProvider keyProvider;
    private final SecureRandom secureRandom;
    private final ObjectMapper objectMapper;

    public SecretProtectionService(KeyEncryptionKeyProvider keyProvider, ObjectMapper objectMapper) {
        this(keyProvider, new SecureRandom(), objectMapper);
    }

    SecretProtectionService(KeyEncryptionKeyProvider keyProvider, SecureRandom secureRandom,
                            ObjectMapper objectMapper) {
        this.keyProvider = Objects.requireNonNull(keyProvider, "keyProvider must not be null");
        this.secureRandom = Objects.requireNonNull(secureRandom, "secureRandom must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        keyProvider.validate();
    }

    public EncryptedSecurityBundle encrypt(java.util.UUID datasourceId, Map<String, String> securityValues) {
        Objects.requireNonNull(datasourceId, "datasourceId must not be null");
        Map<String, String> canonicalValues = canonicalValues(securityValues);
        byte[] dataKeyBytes = new byte[DATA_KEY_LENGTH];
        byte[] nonce = new byte[NONCE_LENGTH];
        secureRandom.nextBytes(dataKeyBytes);
        secureRandom.nextBytes(nonce);

        KeyEncryptionKeyProvider.KeyEncryptionKey key = keyProvider.current();
        try {
            SecretKeySpec dataKey = new SecretKeySpec(dataKeyBytes, "AES");
            Cipher contentCipher = Cipher.getInstance("AES/GCM/NoPadding");
            contentCipher.init(Cipher.ENCRYPT_MODE, dataKey, new GCMParameterSpec(GCM_TAG_LENGTH, nonce));
            contentCipher.updateAAD(associatedData(datasourceId));
            byte[] plaintext = objectMapper.writeValueAsBytes(canonicalValues);
            byte[] ciphertext = contentCipher.doFinal(plaintext);

            Cipher wrapCipher = Cipher.getInstance(KEY_WRAP_ALGORITHM);
            wrapCipher.init(Cipher.WRAP_MODE, key.key());
            byte[] wrappedDataKey = wrapCipher.wrap(dataKey);
            return new EncryptedSecurityBundle(EncryptedSecurityBundle.CURRENT_FORMAT_VERSION,
                    EncryptedSecurityBundle.AES_256_GCM_ALGORITHM, key.version(), wrappedDataKey, nonce, ciphertext);
        } catch (GeneralSecurityException | JsonProcessingException exception) {
            throw new IllegalStateException("Could not encrypt datasource security bundle", exception);
        } finally {
            Arrays.fill(dataKeyBytes, (byte) 0);
        }
    }

    public Map<String, String> decrypt(java.util.UUID datasourceId, EncryptedSecurityBundle bundle) {
        Objects.requireNonNull(datasourceId, "datasourceId must not be null");
        Objects.requireNonNull(bundle, "bundle must not be null");
        if (bundle.formatVersion() != EncryptedSecurityBundle.CURRENT_FORMAT_VERSION
                || !EncryptedSecurityBundle.AES_256_GCM_ALGORITHM.equals(bundle.algorithm())) {
            throw new IllegalArgumentException("Unsupported datasource security bundle");
        }

        KeyEncryptionKeyProvider.KeyEncryptionKey key = keyProvider.find(bundle.keyVersion())
                .orElseThrow(() -> new IllegalArgumentException("Datasource security key version is unavailable"));
        byte[] dataKeyBytes = null;
        try {
            Cipher unwrapCipher = Cipher.getInstance(KEY_WRAP_ALGORITHM);
            unwrapCipher.init(Cipher.UNWRAP_MODE, key.key());
            SecretKeySpec dataKey = (SecretKeySpec) unwrapCipher.unwrap(bundle.wrappedDataKey(), "AES",
                    Cipher.SECRET_KEY);
            dataKeyBytes = dataKey.getEncoded();
            if (dataKeyBytes == null || dataKeyBytes.length != DATA_KEY_LENGTH) {
                throw new IllegalArgumentException("Datasource security bundle has an invalid data key");
            }

            Cipher contentCipher = Cipher.getInstance("AES/GCM/NoPadding");
            contentCipher.init(Cipher.DECRYPT_MODE, dataKey,
                    new GCMParameterSpec(GCM_TAG_LENGTH, bundle.nonce()));
            contentCipher.updateAAD(associatedData(datasourceId));
            byte[] plaintext = contentCipher.doFinal(bundle.ciphertext());
            Map<String, String> values = objectMapper.readValue(plaintext,
                    new TypeReference<Map<String, String>>() { });
            return Map.copyOf(canonicalValues(values));
        } catch (GeneralSecurityException | IOException | RuntimeException exception) {
            if (exception instanceof IllegalArgumentException argumentException
                    && "Datasource security bundle has an invalid data key".equals(argumentException.getMessage())) {
                throw argumentException;
            }
            throw new IllegalArgumentException("Could not decrypt datasource security bundle", exception);
        } finally {
            if (dataKeyBytes != null) {
                Arrays.fill(dataKeyBytes, (byte) 0);
            }
        }
    }

    public EncryptedSecurityBundle reencrypt(java.util.UUID datasourceId, EncryptedSecurityBundle bundle) {
        return encrypt(datasourceId, decrypt(datasourceId, bundle));
    }

    public byte[] serialize(EncryptedSecurityBundle bundle) {
        Objects.requireNonNull(bundle, "bundle must not be null");
        try {
            return objectMapper.writeValueAsBytes(bundle);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize datasource security bundle", exception);
        }
    }

    public EncryptedSecurityBundle deserialize(byte[] serializedBundle) {
        if (serializedBundle == null || serializedBundle.length == 0) {
            throw new IllegalArgumentException("Datasource security bundle must not be empty");
        }
        try {
            return objectMapper.readValue(serializedBundle, EncryptedSecurityBundle.class);
        } catch (IOException | RuntimeException exception) {
            throw new IllegalArgumentException("Could not deserialize datasource security bundle", exception);
        }
    }

    private byte[] associatedData(java.util.UUID datasourceId) {
        return (ASSOCIATED_DATA_PREFIX + datasourceId).getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, String> canonicalValues(Map<String, String> values) {
        Objects.requireNonNull(values, "securityValues must not be null");
        TreeMap<String, String> sorted = new TreeMap<>();
        values.forEach((key, value) -> {
            if (key == null || key.isBlank() || value == null) {
                throw new IllegalArgumentException("security bundle entries must have nonblank keys and values");
            }
            sorted.put(key, value);
        });
        return sorted;
    }
}
