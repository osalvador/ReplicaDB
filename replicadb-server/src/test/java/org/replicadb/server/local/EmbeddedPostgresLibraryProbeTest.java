package org.replicadb.server.local;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("embedded-postgres")
class EmbeddedPostgresLibraryProbeTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void startsPostgresAppliesMigrationsAndInitializesQuartz() throws Exception {
        Path dataDirectory = temporaryDirectory.resolve("postgres-data");
        Path binaryDirectory = temporaryDirectory.resolve("postgres-binaries");

        try (EmbeddedPostgres postgres = newPostgres(dataDirectory, binaryDirectory)) {
            DataSource dataSource = postgres.getPostgresDatabase();
            assertPostgresIsAvailable(dataSource);
            Flyway.configure()
                    .dataSource(dataSource)
                    .locations("classpath:db/migration")
                    .load()
                    .migrate();

            Scheduler scheduler = newScheduler(postgres);
            try {
                scheduler.start();
                assertFalse(scheduler.isInStandbyMode());
            } finally {
                scheduler.shutdown(true);
            }
        }
    }

    @Test
    void reusesTheDataDirectoryAfterRestart() throws Exception {
        Path dataDirectory = temporaryDirectory.resolve("postgres-data");
        Path binaryDirectory = temporaryDirectory.resolve("postgres-binaries");

        try (EmbeddedPostgres postgres = newPostgres(dataDirectory, binaryDirectory);
             Connection connection = postgres.getPostgresDatabase().getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE probe_persistence (id INTEGER PRIMARY KEY)");
            statement.execute("INSERT INTO probe_persistence (id) VALUES (1)");
        }

        try (EmbeddedPostgres postgres = newPostgres(dataDirectory, binaryDirectory);
             Connection connection = postgres.getPostgresDatabase().getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT id FROM probe_persistence")) {
            assertTrue(resultSet.next());
            assertTrue(resultSet.getInt("id") == 1);
        }
    }

    private EmbeddedPostgres newPostgres(Path dataDirectory, Path binaryDirectory) throws Exception {
        return EmbeddedPostgres.builder()
                .setDataDirectory(dataDirectory.toFile())
                .setOverrideWorkingDirectory(binaryDirectory.toFile())
                .setCleanDataDirectory(false)
                .setRegisterShutdownHook(false)
                .setPgBinaryResolver((operatingSystem, architecture) -> {
                    if ("Darwin".equals(operatingSystem) && "aarch64".equals(architecture)) {
                        InputStream binary = getClass().getResourceAsStream("/postgres-darwin-arm_64.txz");
                        if (binary == null) {
                            throw new IllegalStateException("Native Darwin ARM64 binary is not available");
                        }
                        return binary;
                    }
                    return io.zonky.test.db.postgres.embedded.DefaultPostgresBinaryResolver.INSTANCE
                            .getPgBinary(operatingSystem, architecture);
                })
                .setServerConfig("max_connections", "20")
                .setPGStartupWait(java.time.Duration.ofSeconds(30))
                .start();
    }

    private void assertPostgresIsAvailable(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT version()")) {
            assertTrue(resultSet.next());
            assertTrue(resultSet.getString(1).contains("PostgreSQL"));
        }
    }

    private Scheduler newScheduler(EmbeddedPostgres postgres) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("org.quartz.scheduler.instanceName", "EmbeddedPostgresProbe");
        properties.setProperty("org.quartz.scheduler.instanceId", "AUTO");
        properties.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        properties.setProperty("org.quartz.threadPool.threadCount", "1");
        properties.setProperty("org.quartz.threadPool.threadPriority", "5");
        properties.setProperty("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
        properties.setProperty("org.quartz.jobStore.driverDelegateClass",
                "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate");
        properties.setProperty("org.quartz.jobStore.dataSource", "probe");
        properties.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
        properties.setProperty("org.quartz.jobStore.isClustered", "false");
        properties.setProperty("org.quartz.dataSource.probe.provider", "hikaricp");
        properties.setProperty("org.quartz.dataSource.probe.driver", "org.postgresql.Driver");
        properties.setProperty("org.quartz.dataSource.probe.URL", postgres.getJdbcUrl("postgres", "postgres"));
        properties.setProperty("org.quartz.dataSource.probe.user", "postgres");
        properties.setProperty("org.quartz.dataSource.probe.password", "postgres");
        return new StdSchedulerFactory(properties).getScheduler();
    }
}
