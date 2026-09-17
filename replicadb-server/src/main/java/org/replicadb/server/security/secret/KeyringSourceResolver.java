package org.replicadb.server.security.secret;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.Environment;

import java.nio.file.Path;
import java.util.Objects;

public final class KeyringSourceResolver {

    private static final Logger LOG = LogManager.getLogger(KeyringSourceResolver.class);
    private static final String DEPRECATED_FILE_ENVIRONMENT_VARIABLE = "REPLICADB_SECURITY_MASTER_KEY_FILE";

    private KeyringSourceResolver() {
    }

    public static KeyEncryptionKeyProvider resolve(SecretProtectionProperties properties,
                                                    Environment environment,
                                                    ObjectMapper objectMapper) {
        Objects.requireNonNull(properties, "properties must not be null");
        Objects.requireNonNull(environment, "environment must not be null");
        Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        if (environment.getProperty(DEPRECATED_FILE_ENVIRONMENT_VARIABLE) != null) {
            LOG.warn("REPLICADB_SECURITY_MASTER_KEY_FILE is deprecated; "
                    + "use REPLICADB_SECURITY_KEYRING_FILE instead.");
        }

        SecretProtectionProperties.Keyring keyring = Objects.requireNonNull(properties.getKeyring(),
                "replicadb.security.keyring must not be null");
        SecretProtectionProperties.Slot current = Objects.requireNonNull(keyring.getCurrent(),
                "replicadb.security.keyring.current must not be null");
        SecretProtectionProperties.Slot secondary = Objects.requireNonNull(keyring.getSecondary(),
                "replicadb.security.keyring.secondary must not be null");
        String file = keyring.getFile();
        String canonicalFile = environment.getProperty("REPLICADB_SECURITY_KEYRING_FILE");
        String deprecatedFile = environment.getProperty(DEPRECATED_FILE_ENVIRONMENT_VARIABLE);
        if (canonicalFile == null && SecretProtectionProperties.DEFAULT_KEYRING_FILE.equals(file)
                && !isBlank(deprecatedFile)) {
            file = deprecatedFile;
        }
        boolean inlineConfigured = !isBlank(current.getVersion()) || !isBlank(current.getKey())
                || !isBlank(secondary.getVersion()) || !isBlank(secondary.getKey());
        boolean fileExplicit = !SecretProtectionProperties.DEFAULT_KEYRING_FILE.equals(file);
        if (inlineConfigured && fileExplicit) {
            throw new IllegalStateException(
                    "Configure either replicadb.security.keyring.file or inline keyring values, not both");
        }
        if (inlineConfigured) {
            return new EnvBackedKeyEncryptionKeyProvider(current.getVersion(), current.getKey(),
                    secondary.getVersion(), secondary.getKey());
        }
                if (isBlank(file)) {
            throw new IllegalStateException("replicadb.security.keyring.file must not be blank");
        }
                return new FileBackedKeyEncryptionKeyProvider(Path.of(file), objectMapper);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
