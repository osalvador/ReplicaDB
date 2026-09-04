package org.replicadb.server.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.replicadb.server.security.secret.FileBackedKeyEncryptionKeyProvider;
import org.replicadb.server.security.secret.SecretProtectionProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public final class LocalMasterKeyBootstrap {

    private static final String CURRENT_VERSION = "local";

    private final ObjectMapper objectMapper;
    private final SecureRandom secureRandom;

    public LocalMasterKeyBootstrap() {
        this(new ObjectMapper(), new SecureRandom());
    }

    LocalMasterKeyBootstrap(ObjectMapper objectMapper, SecureRandom secureRandom) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.secureRandom = Objects.requireNonNull(secureRandom, "secureRandom must not be null");
    }

    public Path prepare(EmbeddedPostgresHome home, Properties systemProperties,
                        Map<String, String> environment) {
        Objects.requireNonNull(home, "home must not be null");
        Objects.requireNonNull(systemProperties, "systemProperties must not be null");
        Objects.requireNonNull(environment, "environment must not be null");
        home.ensureDirectories();

        Path configuredPath = configuredKeyring(systemProperties, environment);
        Path keyringPath = configuredPath == null ? home.getKeyringFile() : configuredPath;
        if (configuredPath != null) {
            validateKeyring(keyringPath);
            return keyringPath;
        }
        if (Files.exists(keyringPath)) {
            validateKeyring(keyringPath);
            return keyringPath;
        }

        createKeyring(keyringPath);
        validateKeyring(keyringPath);
        return keyringPath;
    }

    private Path configuredKeyring(Properties systemProperties, Map<String, String> environment) {
        String configured = systemProperties.getProperty(SecretProtectionProperties.MASTER_KEY_FILE_PROPERTY);
        if (configured == null) {
            configured = environment.get("REPLICADB_SECURITY_MASTER_KEY_FILE");
        }
        if (configured == null) {
            return null;
        }
        if (configured.isBlank()) {
            throw new IllegalArgumentException(
                    SecretProtectionProperties.MASTER_KEY_FILE_PROPERTY + " must not be blank");
        }
        return Path.of(configured).toAbsolutePath().normalize();
    }

    private void createKeyring(Path keyringPath) {
        try {
            Path parent = keyringPath.getParent();
            if (parent == null) {
                throw new IllegalArgumentException("Keyring path must have a parent directory");
            }
            Files.createDirectories(parent);
            Path temporaryPath = Files.createTempFile(parent, ".master-key-", ".tmp");
            try {
                restrictPermissions(temporaryPath);
                Files.write(temporaryPath, keyringContent(), java.nio.file.StandardOpenOption.WRITE,
                        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
                moveWithoutReplacement(temporaryPath, keyringPath);
            } finally {
                Files.deleteIfExists(temporaryPath);
            }
        } catch (FileAlreadyExistsException exception) {
            validateKeyring(keyringPath);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not create the local datasource encryption keyring", exception);
        }
    }

    private byte[] keyringContent() {
        byte[] key = new byte[32];
        secureRandom.nextBytes(key);
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("currentVersion", CURRENT_VERSION);
            root.putObject("keys").put(CURRENT_VERSION, Base64.getEncoder().encodeToString(key));
            return objectMapper.writeValueAsBytes(root);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not serialize the local datasource encryption keyring", exception);
        } finally {
            java.util.Arrays.fill(key, (byte) 0);
        }
    }

    private void moveWithoutReplacement(Path temporaryPath, Path keyringPath) throws IOException {
        try {
            Files.move(temporaryPath, keyringPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException exception) {
            try {
                Files.move(temporaryPath, keyringPath);
            } catch (FileAlreadyExistsException race) {
                Files.deleteIfExists(temporaryPath);
                validateKeyring(keyringPath);
            }
        }
    }

    private void restrictPermissions(Path path) {
        try {
            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-------");
            Files.setPosixFilePermissions(path, permissions);
        } catch (UnsupportedOperationException ignored) {
        } catch (IOException exception) {
            throw new IllegalStateException("Could not restrict local keyring permissions", exception);
        }
    }

    private void validateKeyring(Path keyringPath) {
        new FileBackedKeyEncryptionKeyProvider(keyringPath, objectMapper).validate();
    }
}
