package org.replicadb.server.security.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.replicadb.server.security.secret.FileBackedKeyEncryptionKeyProvider;
import org.replicadb.server.security.secret.KeyEncryptionKeyProvider;
import org.replicadb.server.security.secret.SecretProtectionProperties;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.nio.file.Path;

@Configuration(proxyBeanMethods = false)
@Profile({"api", "worker"})
@EnableConfigurationProperties(SecretProtectionProperties.class)
public class SecretProtectionConfiguration {

    @Bean
    @ConditionalOnMissingBean(KeyEncryptionKeyProvider.class)
    public KeyEncryptionKeyProvider keyEncryptionKeyProvider(SecretProtectionProperties properties,
                                                             ObjectMapper objectMapper) {
        return new FileBackedKeyEncryptionKeyProvider(Path.of(properties.getMasterKeyFile()), objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public SecretProtectionService secretProtectionService(KeyEncryptionKeyProvider keyProvider,
                                                          ObjectMapper objectMapper) {
        return new SecretProtectionService(keyProvider, objectMapper);
    }
}
