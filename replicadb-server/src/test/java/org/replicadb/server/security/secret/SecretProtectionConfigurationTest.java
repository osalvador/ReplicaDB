package org.replicadb.server.security.secret;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.server.security.config.SecretProtectionConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class SecretProtectionConfigurationTest {

    @Test
    void createsProtectionBeansWithAValidKeyring(@TempDir Path tempDir) throws Exception {
        Path keyring = writeKeyring(tempDir);

        new ApplicationContextRunner()
                .withUserConfiguration(SecretProtectionConfiguration.class)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withPropertyValues("spring.profiles.active=api",
                        "replicadb.security.master-key-file=" + keyring)
                .run(context -> assertThat(context).hasSingleBean(SecretProtectionService.class)
                        .hasSingleBean(KeyEncryptionKeyProvider.class));
    }

    @Test
    void failsContextStartupWhenTheKeyringIsUnavailable(@TempDir Path tempDir) {
        new ApplicationContextRunner()
                .withUserConfiguration(SecretProtectionConfiguration.class)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withPropertyValues("spring.profiles.active=worker",
                        "replicadb.security.master-key-file=" + tempDir.resolve("missing.json"))
                .run(context -> assertThat(context.getStartupFailure())
                        .hasMessageContaining("Datasource encryption keyring is unavailable"));
    }

    private static Path writeKeyring(Path tempDir) throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        SecretKey key = generator.generateKey();
        String content = "{\"currentVersion\":\"v1\",\"keys\":{\"v1\":\""
                + Base64.getEncoder().encodeToString(key.getEncoded()) + "\"}}";
        Path keyring = tempDir.resolve("keyring.json");
        Files.writeString(keyring, content, StandardCharsets.UTF_8);
        return keyring;
    }
}
