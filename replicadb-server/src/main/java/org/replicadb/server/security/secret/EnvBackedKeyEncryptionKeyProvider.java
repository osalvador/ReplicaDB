package org.replicadb.server.security.secret;

import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class EnvBackedKeyEncryptionKeyProvider implements KeyEncryptionKeyProvider {

    private static final String CURRENT_KEY_VARIABLE = "REPLICADB_SECURITY_KEYRING_CURRENT_KEY";
    private static final String SECONDARY_KEY_VARIABLE = "REPLICADB_SECURITY_KEYRING_SECONDARY_KEY";

    private final String currentVersion;
    private final Map<String, KeyEncryptionKey> keys;

    public EnvBackedKeyEncryptionKeyProvider(String currentVersion, String currentKeyBase64,
                                              String secondaryVersion, String secondaryKeyBase64) {
        this.currentVersion = requireVersion(currentVersion, "REPLICADB_SECURITY_KEYRING_CURRENT_VERSION");
        Map<String, KeyEncryptionKey> loadedKeys = new LinkedHashMap<>();
        loadedKeys.put(this.currentVersion,
                new KeyEncryptionKey(this.currentVersion,
                        new SecretKeySpec(decode(CURRENT_KEY_VARIABLE, currentKeyBase64), "AES")));

        boolean secondaryVersionConfigured = !isBlank(secondaryVersion);
        boolean secondaryKeyConfigured = !isBlank(secondaryKeyBase64);
        if (secondaryVersionConfigured != secondaryKeyConfigured) {
            throw new IllegalStateException(
                    "REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION and "
                            + SECONDARY_KEY_VARIABLE + " must be configured together");
        }
        if (secondaryVersionConfigured) {
            String normalizedSecondaryVersion = secondaryVersion.strip();
            if (this.currentVersion.equals(normalizedSecondaryVersion)) {
                throw new IllegalStateException(
                        "REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION must differ from "
                                + "REPLICADB_SECURITY_KEYRING_CURRENT_VERSION");
            }
            loadedKeys.put(normalizedSecondaryVersion,
                    new KeyEncryptionKey(normalizedSecondaryVersion,
                            new SecretKeySpec(decode(SECONDARY_KEY_VARIABLE, secondaryKeyBase64), "AES")));
        }
        this.keys = Map.copyOf(loadedKeys);
    }

    @Override
    public KeyEncryptionKey current() {
        return Objects.requireNonNull(keys.get(currentVersion));
    }

    @Override
    public Optional<KeyEncryptionKey> find(String version) {
        return Optional.ofNullable(keys.get(version));
    }

    @Override
    public Set<String> knownVersions() {
        return keys.keySet();
    }

    private static String requireVersion(String version, String variableName) {
        if (isBlank(version)) {
            throw new IllegalStateException(variableName + " must not be blank");
        }
        return version.strip();
    }

    private static byte[] decode(String variableName, String value) {
        if (isBlank(value)) {
            throw new IllegalStateException(variableName + " must not be blank");
        }
        String normalized = value.strip();
        final byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(normalized);
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException(variableName + " is not valid Base64", exception);
        }
        if (decoded.length != 32) {
            throw new IllegalStateException(variableName + " decodes to " + decoded.length
                    + " bytes; 32 are required");
        }
        return decoded;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
