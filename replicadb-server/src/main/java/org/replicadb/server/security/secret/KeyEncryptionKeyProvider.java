package org.replicadb.server.security.secret;

import javax.crypto.SecretKey;
import java.util.Objects;
import java.util.Optional;

public interface KeyEncryptionKeyProvider {

    KeyEncryptionKey current();

    Optional<KeyEncryptionKey> find(String version);

    default void validate() {
        KeyEncryptionKey key = current();
        Objects.requireNonNull(key, "current key must not be null");
    }

    record KeyEncryptionKey(String version, SecretKey key) {
        public KeyEncryptionKey {
            if (version == null || version.isBlank()) {
                throw new IllegalArgumentException("version must not be blank");
            }
            Objects.requireNonNull(key, "key must not be null");
            if (!"AES".equalsIgnoreCase(key.getAlgorithm()) || key.getEncoded() == null
                    || key.getEncoded().length != 32) {
                throw new IllegalArgumentException("key must be a 256-bit AES key");
            }
        }
    }
}
