package org.replicadb.server.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.replicadb.server.security.secret.FileBackedKeyEncryptionKeyProvider;
import org.replicadb.server.security.secret.SecretProtectionProperties;
import org.replicadb.server.security.secret.SecretProtectionService;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalMasterKeyBootstrapTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void createsA256BitKeyringAndPreservesItAcrossRestart() throws Exception {
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(temporaryDirectory.resolve("replicadb"));
        LocalMasterKeyBootstrap bootstrap = new LocalMasterKeyBootstrap();
        Properties properties = new Properties();
        Path keyring = bootstrap.prepare(home, properties, Map.of());
        String initialContent = Files.readString(keyring, StandardCharsets.UTF_8);
        FileBackedKeyEncryptionKeyProvider provider = new FileBackedKeyEncryptionKeyProvider(keyring,
                new ObjectMapper());
        UUID datasourceId = UUID.randomUUID();
        SecretProtectionService protectionService = new SecretProtectionService(provider, new ObjectMapper());
        EncryptedSecurityBundle bundle = protectionService.encrypt(datasourceId, Map.of("password", "value"));

        Path restartedKeyring = bootstrap.prepare(home, properties, Map.of());
        FileBackedKeyEncryptionKeyProvider restartedProvider = new FileBackedKeyEncryptionKeyProvider(
                restartedKeyring, new ObjectMapper());

        assertEquals(keyring, restartedKeyring);
        assertEquals(initialContent, Files.readString(restartedKeyring, StandardCharsets.UTF_8));
        assertEquals(32, restartedProvider.current().key().getEncoded().length);
        assertEquals("AES", restartedProvider.current().key().getAlgorithm());
        assertEquals(Map.of("password", "value"),
                new SecretProtectionService(restartedProvider, new ObjectMapper()).decrypt(datasourceId, bundle));
        assertRestrictivePermissions(keyring);
    }

    @Test
    void createsDifferentKeysForDifferentHomes() throws Exception {
        LocalMasterKeyBootstrap bootstrap = new LocalMasterKeyBootstrap();
        Path first = bootstrap.prepare(EmbeddedPostgresHome.from(temporaryDirectory.resolve("one")),
                new Properties(), Map.of());
        Path second = bootstrap.prepare(EmbeddedPostgresHome.from(temporaryDirectory.resolve("two")),
                new Properties(), Map.of());

        assertNotEquals(Files.readString(first), Files.readString(second));
    }

    @Test
    void respectsAnExplicitKeyringPathWithoutCreatingTheLocalOne() throws Exception {
        Path explicitPath = temporaryDirectory.resolve("external-keyring.json");
        writeKeyring(explicitPath);
        Properties properties = new Properties();
        properties.setProperty(SecretProtectionProperties.MASTER_KEY_FILE_PROPERTY, explicitPath.toString());
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(temporaryDirectory.resolve("replicadb"));

        Path result = new LocalMasterKeyBootstrap().prepare(home, properties, Map.of());

        assertEquals(explicitPath.toAbsolutePath(), result);
        assertFalse(Files.exists(home.getKeyringFile()));
    }

    @Test
    void rejectsMalformedExistingKeyringsAndInvalidConfiguration() throws Exception {
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(temporaryDirectory.resolve("replicadb"));
        home.ensureDirectories();
        Files.writeString(home.getKeyringFile(), "not-json", StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class,
                () -> new LocalMasterKeyBootstrap().prepare(home, new Properties(), Map.of()));

        Properties blankPath = new Properties();
        blankPath.setProperty(SecretProtectionProperties.MASTER_KEY_FILE_PROPERTY, " ");
        assertThrows(IllegalArgumentException.class,
                () -> new LocalMasterKeyBootstrap().prepare(home, blankPath, Map.of()));

        Path invalidKeyring = temporaryDirectory.resolve("invalid-keyring.json");
        Files.writeString(invalidKeyring,
                "{\"currentVersion\":\"local\",\"keys\":{\"local\":\"AQ==\"}}",
                StandardCharsets.UTF_8);
        Properties invalidPath = new Properties();
        invalidPath.setProperty(SecretProtectionProperties.MASTER_KEY_FILE_PROPERTY,
                invalidKeyring.toString());
        assertThrows(IllegalStateException.class,
                () -> new LocalMasterKeyBootstrap().prepare(home, invalidPath, Map.of()));
    }

    @Test
    void rejectsAHomeWithAConflictingSecurityFile() throws Exception {
        Path root = temporaryDirectory.resolve("replicadb");
        Files.createDirectories(root);
        Files.writeString(root.resolve("security"), "file", StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class,
                () -> new LocalMasterKeyBootstrap().prepare(EmbeddedPostgresHome.from(root),
                        new Properties(), Map.of()));
    }

    @Test
    void doesNotExposeKeyMaterialInValidationErrors() throws Exception {
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(temporaryDirectory.resolve("replicadb"));
        home.ensureDirectories();
        String sensitiveValue = "not-a-real-secret-value";
        Files.writeString(home.getKeyringFile(),
                "{\"currentVersion\":\"local\",\"keys\":{\"local\":\"" + sensitiveValue + "\"}}",
                StandardCharsets.UTF_8);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new LocalMasterKeyBootstrap().prepare(home, new Properties(), Map.of()));

        assertFalse(exception.getMessage().contains(sensitiveValue));
    }

    private void assertRestrictivePermissions(Path keyring) throws Exception {
        PosixFileAttributeView view = Files.getFileAttributeView(keyring, PosixFileAttributeView.class);
        if (view != null) {
            assertEquals(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
                    view.readAttributes().permissions());
        }
    }

    private void writeKeyring(Path path) throws Exception {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        Files.createDirectories(path.getParent());
        Files.writeString(path, "{\"currentVersion\":\"local\",\"keys\":{\"local\":\""
                + Base64.getEncoder().encodeToString(key) + "\"}}", StandardCharsets.UTF_8);
    }
}
