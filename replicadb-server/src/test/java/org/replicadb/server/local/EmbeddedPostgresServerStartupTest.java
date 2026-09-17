package org.replicadb.server.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedPostgresServerStartupTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void resolvesCommandLineOptionsWithCommandLinePrecedence() {
        Properties systemProperties = baseSystemProperties();
        systemProperties.setProperty(EmbeddedPostgresProperties.PORT_PROPERTY, "5432");
        String[] arguments = {
                "--replicadb.embedded-postgres.enabled=true",
                "--replicadb.server.home=" + temporaryDirectory.resolve("command-line-home"),
                "--replicadb.embedded-postgres.port=5433",
                "--spring.profiles.active=api"
        };

        EmbeddedPostgresLaunchOptions options = EmbeddedPostgresLaunchOptions.resolve(
                arguments, systemProperties, Map.of());

        assertTrue(options.isEmbeddedPostgresEnabled());
        assertEquals(5433, options.getEmbeddedPostgresProperties().getPort());
        assertArrayEquals(arguments, options.getArguments());
        assertFalse(options.getResolvedProperties().getProperty("spring.profiles.active").isBlank());
    }

    @ParameterizedTest
    @ValueSource(strings = {"worker", "worker,api"})
    void rejectsWorkerProfileWhenEmbeddedPostgresIsEnabled(String profile) {
        String[] arguments = {
                "--replicadb.embedded-postgres.enabled=true",
                "--spring.profiles.active=" + profile
        };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> EmbeddedPostgresLaunchOptions.resolve(arguments, baseSystemProperties(), Map.of()));

        assertTrue(exception.getMessage().contains("worker"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "--DB_URL=jdbc:postgresql://localhost/metadata?password=hidden-value",
            "--DB_USERNAME=metadata-user",
            "--DB_PASSWORD=hidden-value",
            "--spring.datasource.url=jdbc:postgresql://localhost/metadata?password=hidden-value"
    })
    void rejectsExternalDatasourceArgumentsWithoutLeakingValues(String externalDatasourceArgument) {
        String[] arguments = {
                "--replicadb.embedded-postgres.enabled=true",
                externalDatasourceArgument
        };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> EmbeddedPostgresLaunchOptions.resolve(arguments, baseSystemProperties(), Map.of()));

        assertFalse(exception.getMessage().contains("hidden-value"));
    }

        @Test
        void rejectsBootstrapPasswordOnTheCommandLine() {
                String[] arguments = {
                                "--replicadb.embedded-postgres.enabled=true",
                                "--REPLICADB_BOOTSTRAP_ADMIN_PASSWORD=hidden-value"
                };

                IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                                () -> EmbeddedPostgresLaunchOptions.resolve(arguments, baseSystemProperties(), Map.of()));

                assertFalse(exception.getMessage().contains("hidden-value"));
        }

    @Test
    void rejectsDisabledLocalExecution() {
        String[] arguments = {
                "--replicadb.embedded-postgres.enabled=true",
                "--replicadb.server.local-execution.enabled=false"
        };

        assertThrows(IllegalArgumentException.class,
                () -> EmbeddedPostgresLaunchOptions.resolve(arguments, baseSystemProperties(), Map.of()));
    }

    @Test
    void leavesTheNormalWorkerLaunchPathUntouched() {
        String[] arguments = {"--spring.profiles.active=worker"};

        EmbeddedPostgresLaunchOptions options = EmbeddedPostgresLaunchOptions.resolve(
                arguments, baseSystemProperties(), Map.of());

        assertFalse(options.isEmbeddedPostgresEnabled());
    }

    private Properties baseSystemProperties() {
        Properties properties = new Properties();
        properties.setProperty("user.home", temporaryDirectory.toString());
        return properties;
    }

}
