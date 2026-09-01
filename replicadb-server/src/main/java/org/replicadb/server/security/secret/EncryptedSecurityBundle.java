package org.replicadb.server.security.secret;

import java.util.Arrays;
import java.util.Objects;

public record EncryptedSecurityBundle(
        int formatVersion,
        String algorithm,
        String keyVersion,
        byte[] wrappedDataKey,
        byte[] nonce,
        byte[] ciphertext) {

    public static final int CURRENT_FORMAT_VERSION = 1;
    public static final String AES_256_GCM_ALGORITHM = "AES-256-GCM";

    public EncryptedSecurityBundle {
        if (formatVersion < 1) {
            throw new IllegalArgumentException("formatVersion must be positive");
        }
        requireNonBlank("algorithm", algorithm);
        requireNonBlank("keyVersion", keyVersion);
        wrappedDataKey = copyRequired("wrappedDataKey", wrappedDataKey);
        nonce = copyRequired("nonce", nonce);
        ciphertext = copyRequired("ciphertext", ciphertext);
    }

    @Override
    public byte[] wrappedDataKey() {
        return Arrays.copyOf(wrappedDataKey, wrappedDataKey.length);
    }

    @Override
    public byte[] nonce() {
        return Arrays.copyOf(nonce, nonce.length);
    }

    @Override
    public byte[] ciphertext() {
        return Arrays.copyOf(ciphertext, ciphertext.length);
    }

    private static byte[] copyRequired(String fieldName, byte[] value) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.length == 0) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        return Arrays.copyOf(value, value.length);
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
