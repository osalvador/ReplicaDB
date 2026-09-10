package org.replicadb.server.security.secret;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyringSourceResolverTest {

    @Test
    void resolvesAnExplicitFileWhenInlineValuesAreAbsent(@TempDir Path tempDir) throws Exception {
        SecretProtectionProperties properties = new SecretProtectionProperties();
        properties.getKeyring().setFile(writeKeyring(tempDir).toString());

        KeyEncryptionKeyProvider provider = KeyringSourceResolver.resolve(properties,
                new MockEnvironment(), new ObjectMapper());

        assertThat(provider).isInstanceOf(FileBackedKeyEncryptionKeyProvider.class);
    }

    @Test
    void resolvesInlineValuesWhenTheFileUsesItsDefault(@TempDir Path tempDir) throws Exception {
        SecretProtectionProperties properties = new SecretProtectionProperties();
        properties.getKeyring().getCurrent().setVersion("v1");
        properties.getKeyring().getCurrent().setKey(encodedKey());

        KeyEncryptionKeyProvider provider = KeyringSourceResolver.resolve(properties,
                new MockEnvironment(), new ObjectMapper());

        assertThat(provider).isInstanceOf(EnvBackedKeyEncryptionKeyProvider.class);
    }

    @Test
    void rejectsFileAndInlineConfigurationTogether(@TempDir Path tempDir) throws Exception {
        SecretProtectionProperties properties = new SecretProtectionProperties();
        properties.getKeyring().setFile(writeKeyring(tempDir).toString());
        properties.getKeyring().getCurrent().setVersion("v1");
        properties.getKeyring().getCurrent().setKey(encodedKey());

        assertThatThrownBy(() -> KeyringSourceResolver.resolve(properties,
                new MockEnvironment(), new ObjectMapper()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("keyring.file")
                .hasMessageContaining("inline");
    }

    @Test
    void acceptsDeprecatedFileEnvironmentVariableWithWarning(@TempDir Path tempDir) throws Exception {
        SecretProtectionProperties properties = new SecretProtectionProperties();
        properties.getKeyring().setFile(writeKeyring(tempDir).toString());
        MockEnvironment environment = new MockEnvironment()
                .withProperty("REPLICADB_SECURITY_MASTER_KEY_FILE", properties.getKeyring().getFile());

        KeyEncryptionKeyProvider provider = KeyringSourceResolver.resolve(properties, environment,
                new ObjectMapper());

        assertThat(provider).isInstanceOf(FileBackedKeyEncryptionKeyProvider.class);
    }

    private static Path writeKeyring(Path tempDir) throws Exception {
        String content = "{\"currentVersion\":\"v1\",\"keys\":{\"v1\":\""
                + encodedKey() + "\"}}";
        Path keyring = tempDir.resolve("keyring.json");
        Files.writeString(keyring, content, StandardCharsets.UTF_8);
        return keyring;
    }

    private static String encodedKey() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        SecretKey key = generator.generateKey();
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }
}
