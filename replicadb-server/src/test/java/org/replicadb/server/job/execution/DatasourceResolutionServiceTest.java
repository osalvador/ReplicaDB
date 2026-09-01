package org.replicadb.server.job.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ResolvedDataSource;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.replicadb.server.security.secret.KeyEncryptionKeyProvider;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.replicadb.server.security.secret.KeyEncryptionKeyProvider.KeyEncryptionKey;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatasourceResolutionServiceTest {

    @Test
    void decryptsEachDistinctDatasourceOnceAndBuildsAllManagedOptions() {
        SecretProtectionService protectionService = protectionService();
        DatasourceResolutionService resolutionService = new DatasourceResolutionService(protectionService);
        ManagedDataSource source = dataSource(protectionService, UUID.randomUUID(), "source", Map.of(
                "connect", "jdbc:postgresql://${env:LITERAL_CONNECT}",
                "user", "source-user",
                "password", "${env:LITERAL_PASSWORD}",
                "auth.mode", "ActiveDirectoryDefault",
                "connect.parameter.clientId", "client-id"));
        JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
                .withSourceDatasourceId(source.id())
                .withSinkDatasourceId(source.id())
                .withSourceTable("source_table")
                .withSinkTable("sink_table")
                .withMode(org.replicadb.cli.ReplicationMode.INCREMENTAL)
                .withIncrementalWatermarkColumn("updated_at")
                .withInitialWatermarkValue("0")
                .withFetchSize(250)
                .withBandwidthThrottling(512)
                .withVerbose(true)
                .build();
        ClaimedRunPreparation preparation = preparation(definition, source, source);

        ResolvedJobDefinition resolved = resolutionService.resolve(preparation);
        assertEquals(resolved.sourceDataSource(), resolved.sinkDataSource());
        assertEquals("source-user", resolved.sourceDataSource().user());
        assertEquals("${env:LITERAL_PASSWORD}", resolved.sourceDataSource().password());
        assertEquals("ActiveDirectoryDefault", resolved.sourceDataSource().authentication().mode());
        assertEquals("client-id", resolved.sourceDataSource().securityParams()
            .get("connect.parameter.clientId"));

        ToolOptions options = new ManagedToolOptionsFactory().build(resolved, "10");
        assertEquals("jdbc:postgresql://${env:LITERAL_CONNECT}", options.getSourceConnect());
        assertEquals("jdbc:postgresql://${env:LITERAL_CONNECT}", options.getSinkConnect());
        assertEquals("${env:LITERAL_PASSWORD}", options.getSourcePassword());
        assertEquals("source-user", options.getSinkUser());
        assertEquals("client-id", options.getSourceConnectionParams().getProperty("clientId"));
        assertEquals("client-id", options.getSinkConnectionParams().getProperty("clientId"));
        assertEquals("ActiveDirectoryDefault", options.getSourceAuthentication().getMode().toString());
        assertEquals("incremental", options.getMode());
        assertEquals(250, options.getFetchSize());
        assertEquals(512, options.getBandwidthThrottling());
        assertEquals(org.apache.logging.log4j.Level.DEBUG, options.getVerboseLevel());
        assertEquals("10", options.getIncrementalWatermarkValue());
    }

    @Test
    void keepsLiteralValuesAndCreatesNoManagedFiles(@TempDir Path temporaryDirectory) throws Exception {
        SecretProtectionService protectionService = protectionService();
        ManagedDataSource source = dataSource(protectionService, UUID.randomUUID(), "source", Map.of(
                "connect", "jdbc:sqlite:${env:SOURCE_CONNECT}", "connect.parameter.path", "${literal}"));
        ManagedDataSource sink = dataSource(protectionService, UUID.randomUUID(), "sink", Map.of(
                "connect", "jdbc:sqlite:${env:SINK_CONNECT}"));
        JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
                .withSourceDatasourceId(source.id())
                .withSinkDatasourceId(sink.id())
                .build();

        ToolOptions options = new ManagedToolOptionsFactory().build(
                new DatasourceResolutionService(protectionService).resolve(preparation(definition, source, sink)), null);

        assertEquals("jdbc:sqlite:${env:SOURCE_CONNECT}", options.getSourceConnect());
        assertEquals("${literal}", options.getSourceConnectionParams().getProperty("path"));
        assertEquals("jdbc:sqlite:${env:SINK_CONNECT}", options.getSinkConnect());
        assertTrue(Files.list(temporaryDirectory).findAny().isEmpty());
    }

    @Test
    void rejectsTamperedOrMalformedBundlesBeforeMaterialization() {
        SecretProtectionService protectionService = protectionService();
        UUID id = UUID.randomUUID();
        ManagedDataSource source = dataSource(protectionService, id, "source",
                Map.of("connect", "jdbc:postgresql://host/db"));
        byte[] serialized = source.encryptedSecurity();
        serialized[serialized.length - 1] ^= 1;
        ManagedDataSource tampered = new ManagedDataSource(id, source.name(), source.connectorType(),
                source.safeConnectDisplay(), source.technicalParams(), serialized,
                source.securityFormatVersion(), source.encryptionAlgorithm(), source.keyVersion(),
                source.createdAt(), source.updatedAt());
        JobDefinition definition = JobDefinitionTestFixtures.aJobDefinition()
                .withSourceDatasourceId(id)
                .withSinkDatasourceId(id)
                .build();

        assertThrows(IllegalArgumentException.class, () -> new DatasourceResolutionService(protectionService)
                .resolve(preparation(definition, tampered, tampered)));
        assertNotNull(source.encryptedSecurity());
        assertFalse(source.encryptedSecurity().length == 0);
    }

    private static ClaimedRunPreparation preparation(JobDefinition definition,
                                                     ManagedDataSource source,
                                                     ManagedDataSource sink) {
        Instant now = Instant.now();
        JobRun run = new JobRun(UUID.randomUUID(), definition.id(), null, JobRunStatus.RUNNING, 1,
                "worker", now.plusSeconds(300), now, now, now, null, null, null,
                null, null, null, now, LeaseToken.generate(), source.id(), sink.id(), now);
        return new ClaimedRunPreparation(run, definition, source, sink);
    }

    private static ManagedDataSource dataSource(SecretProtectionService protectionService, UUID id,
                                                String name, Map<String, String> security) {
        EncryptedSecurityBundle bundle = protectionService.encrypt(id, security);
        return new ManagedDataSource(id, name, ConnectorType.POSTGRES,
                "jdbc:postgresql://[REDACTED]/db", Map.of("sslmode", "require"),
                protectionService.serialize(bundle), bundle.formatVersion(), bundle.algorithm(),
                bundle.keyVersion(), null, null);
    }

    private static SecretProtectionService protectionService() {
        KeyEncryptionKey key = key("test");
        KeyEncryptionKeyProvider provider = new KeyEncryptionKeyProvider() {
            @Override
            public KeyEncryptionKey current() {
                return key;
            }

            @Override
            public Optional<KeyEncryptionKey> find(String version) {
                return key.version().equals(version) ? Optional.of(key) : Optional.empty();
            }
        };
        return new SecretProtectionService(provider, new ObjectMapper());
    }

    private static KeyEncryptionKeyProvider.KeyEncryptionKey key(String version) {
        try {
            KeyGenerator generator = KeyGenerator.getInstance("AES");
            generator.init(256);
            SecretKey key = generator.generateKey();
            return new KeyEncryptionKeyProvider.KeyEncryptionKey(version, key);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
