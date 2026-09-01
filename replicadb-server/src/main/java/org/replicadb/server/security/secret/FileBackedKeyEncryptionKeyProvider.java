package org.replicadb.server.security.secret;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class FileBackedKeyEncryptionKeyProvider implements KeyEncryptionKeyProvider {

    private final String currentVersion;
    private final Map<String, KeyEncryptionKey> keys;

    public FileBackedKeyEncryptionKeyProvider(Path keyringPath, ObjectMapper objectMapper) {
        Objects.requireNonNull(keyringPath, "keyringPath must not be null");
        Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        JsonNode root = readKeyring(keyringPath, objectMapper);
        this.currentVersion = text(root, "currentVersion");
        this.keys = loadKeys(root);
        if (!keys.containsKey(currentVersion)) {
            throw new IllegalStateException("Datasource encryption keyring has no current key");
        }
    }

    @Override
    public KeyEncryptionKey current() {
        return keys.get(currentVersion);
    }

    @Override
    public Optional<KeyEncryptionKey> find(String version) {
        return Optional.ofNullable(keys.get(version));
    }

    private static JsonNode readKeyring(Path path, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(Files.readString(path, StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException exception) {
            throw new IllegalStateException("Datasource encryption keyring is unavailable", exception);
        }
    }

    private static Map<String, KeyEncryptionKey> loadKeys(JsonNode root) {
        JsonNode keysNode = root == null ? null : root.get("keys");
        if (keysNode == null || !keysNode.isObject() || keysNode.isEmpty()) {
            throw new IllegalStateException("Datasource encryption keyring contains no keys");
        }

        Map<String, KeyEncryptionKey> result = new LinkedHashMap<>();
        keysNode.fields().forEachRemaining(entry -> {
            String version = entry.getKey();
            String encoded = entry.getValue().isTextual() ? entry.getValue().textValue() : null;
            if (encoded == null || encoded.isBlank()) {
                throw new IllegalStateException("Datasource encryption keyring contains an invalid key");
            }
            final byte[] keyBytes;
            try {
                keyBytes = Base64.getDecoder().decode(encoded);
            } catch (IllegalArgumentException exception) {
                throw new IllegalStateException("Datasource encryption keyring contains an invalid key", exception);
            }
            if (keyBytes.length != 32) {
                throw new IllegalStateException("Datasource encryption keyring contains a non-256-bit key");
            }
            result.put(version, new KeyEncryptionKey(version, new SecretKeySpec(keyBytes, "AES")));
        });
        return Map.copyOf(result);
    }

    private static String text(JsonNode root, String fieldName) {
        JsonNode value = root == null ? null : root.get(fieldName);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalStateException("Datasource encryption keyring is missing " + fieldName);
        }
        return value.textValue();
    }
}
