package org.replicadb.server.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedPostgresPropertiesTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void usesReplicadbHomeUnderUserHomeByDefault() {
        Properties systemProperties = propertiesWithUserHome();

        EmbeddedPostgresProperties properties = EmbeddedPostgresProperties.resolve(systemProperties, Map.of());

        assertFalse(properties.isEnabled());
        assertEquals(temporaryDirectory.resolve(".replicadb"), properties.getHome().getRoot());
        assertEquals("14.22.0", properties.getPostgresVersion());
        assertEquals(0, properties.getPort());
        assertEquals(java.time.Duration.ofMinutes(2), properties.getStartupTimeout());
        assertEquals(java.time.Duration.ofMinutes(2), properties.getDownloadTimeout());
        assertEquals(3, properties.getDownloadRetries());
    }

    @Test
    void resolvesEnvironmentValuesAndCreatesTheExpectedDirectories() {
        Path configuredHome = temporaryDirectory.resolve("folder with spaces");
        Map<String, String> environment = Map.of(
                "REPLICADB_EMBEDDED_POSTGRES_ENABLED", "true",
                "REPLICADB_SERVER_HOME", configuredHome.toString(),
                "REPLICADB_EMBEDDED_POSTGRES_VERSION", "15.2.1",
                "REPLICADB_EMBEDDED_POSTGRES_PORT", "5433",
                "REPLICADB_EMBEDDED_POSTGRES_STARTUP_TIMEOUT", "5s",
                "REPLICADB_EMBEDDED_POSTGRES_DOWNLOAD_TIMEOUT", "2500ms",
                "REPLICADB_EMBEDDED_POSTGRES_DOWNLOAD_RETRIES", "4");

        EmbeddedPostgresProperties properties = EmbeddedPostgresProperties.resolve(propertiesWithUserHome(), environment);
        properties.getHome().ensureDirectories();

        assertTrue(properties.isEnabled());
        assertEquals(configuredHome.toAbsolutePath(), properties.getHome().getRoot());
        assertEquals(15, Integer.parseInt(properties.getPostgresVersion().substring(0, 2)));
        assertEquals(5433, properties.getPort());
        assertEquals(java.time.Duration.ofSeconds(5), properties.getStartupTimeout());
        assertEquals(java.time.Duration.ofMillis(2500), properties.getDownloadTimeout());
        assertEquals(4, properties.getDownloadRetries());
        assertTrue(Files.isDirectory(properties.getHome().getDataDirectory()));
        assertTrue(Files.isDirectory(properties.getHome().getCacheDirectory()));
        assertTrue(Files.isDirectory(properties.getHome().getSecurityDirectory()));
        assertTrue(Files.isDirectory(properties.getHome().getLocksDirectory()));
        assertTrue(Files.isDirectory(properties.getHome().getRunDirectory()));
        assertTrue(Files.isDirectory(properties.getHome().getLogsDirectory()));
    }

    @Test
    void systemPropertiesTakePrecedenceOverEnvironment() {
        Properties systemProperties = propertiesWithUserHome();
        systemProperties.setProperty(EmbeddedPostgresProperties.HOME_PROPERTY,
                temporaryDirectory.resolve("system-home").toString());
        systemProperties.setProperty(EmbeddedPostgresProperties.PORT_PROPERTY, "5434");
        systemProperties.setProperty(EmbeddedPostgresProperties.ENABLED_PROPERTY, "false");

        Map<String, String> environment = Map.of(
                EmbeddedPostgresProperties.HOME_ENVIRONMENT, temporaryDirectory.resolve("env-home").toString(),
                EmbeddedPostgresProperties.PORT_ENVIRONMENT, "5433",
                EmbeddedPostgresProperties.ENABLED_ENVIRONMENT, "true");

        EmbeddedPostgresProperties properties = EmbeddedPostgresProperties.resolve(systemProperties, environment);

        assertFalse(properties.isEnabled());
        assertEquals(5434, properties.getPort());
        assertEquals(temporaryDirectory.resolve("system-home").toAbsolutePath(), properties.getHome().getRoot());
    }

        @Test
        void rejectsCliHomeEnvironmentInsteadOfRedirectingServerState() {
                Map<String, String> environment = Map.of(
                                EmbeddedPostgresProperties.LEGACY_HOME_ENVIRONMENT,
                                temporaryDirectory.resolve("cli-home").toString());

                IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                                () -> EmbeddedPostgresProperties.resolve(propertiesWithUserHome(), environment));

                assertTrue(exception.getMessage().contains("REPLICADB_SERVER_HOME"));
                assertFalse(exception.getMessage().contains("cli-home"));
        }

    @ParameterizedTest
    @ValueSource(strings = {
            "DB_URL",
            "DB_USERNAME",
            "DB_PASSWORD"
    })
    void rejectsExternalDatasourceConfigurationWhenEmbeddedModeIsEnabled(String key) {
        Map<String, String> environment = new HashMap<>();
        environment.put(EmbeddedPostgresProperties.ENABLED_ENVIRONMENT, "true");
        environment.put(key, "password=do-not-leak-this-value");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> EmbeddedPostgresProperties.resolve(propertiesWithUserHome(), environment));

        assertTrue(exception.getMessage().contains(key));
        assertFalse(exception.getMessage().contains("do-not-leak-this-value"));
    }

    @Test
    void allowsExternalDatasourceConfigurationWhenEmbeddedModeIsDisabled() {
        Map<String, String> environment = Map.of(
                EmbeddedPostgresProperties.ENABLED_ENVIRONMENT, "false",
                "DB_URL", "jdbc:postgresql://localhost/metadata");

        EmbeddedPostgresProperties properties = EmbeddedPostgresProperties.resolve(propertiesWithUserHome(), environment);

        assertFalse(properties.isEnabled());
    }

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> resolveWith(EmbeddedPostgresProperties.PORT_PROPERTY, "65536"));
        assertThrows(IllegalArgumentException.class,
                () -> resolveWith(EmbeddedPostgresProperties.PORT_PROPERTY, "-1"));
        assertThrows(IllegalArgumentException.class,
                () -> resolveWith(EmbeddedPostgresProperties.STARTUP_TIMEOUT_PROPERTY, "0s"));
        assertThrows(IllegalArgumentException.class,
                () -> resolveWith(EmbeddedPostgresProperties.DOWNLOAD_RETRIES_PROPERTY, "-1"));
        assertThrows(IllegalArgumentException.class,
                () -> resolveWith(EmbeddedPostgresProperties.ENABLED_PROPERTY, "yes"));
        assertThrows(IllegalArgumentException.class,
                () -> resolveWith(EmbeddedPostgresProperties.VERSION_PROPERTY, " "));
    }

    @Test
    void rejectsAHomeThatCannotBeUsedAsADirectory() throws Exception {
        Path file = temporaryDirectory.resolve("home-file");
        Files.createFile(file);
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(file);

        assertThrows(IllegalStateException.class, home::ensureDirectories);
    }

    @Test
    void rejectsBlankHome() {
        assertThrows(IllegalArgumentException.class, () -> EmbeddedPostgresHome.from(Path.of("")));
    }

    private Properties propertiesWithUserHome() {
        Properties properties = new Properties();
        properties.setProperty("user.home", temporaryDirectory.toString());
        return properties;
    }

    private EmbeddedPostgresProperties resolveWith(String key, String value) {
        Properties properties = propertiesWithUserHome();
        properties.setProperty(key, value);
        return EmbeddedPostgresProperties.resolve(properties, Map.of());
    }
}
