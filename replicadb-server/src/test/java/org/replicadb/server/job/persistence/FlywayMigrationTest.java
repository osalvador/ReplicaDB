package org.replicadb.server.job.persistence;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FlywayMigrationTest {

    @Container
        static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    @Test
    void appliesAndValidatesMetadataMigrations() {
        Flyway flyway = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load();

        assertEquals(12, flyway.migrate().migrationsExecuted);
        assertEquals(12, flyway.info().applied().length);
        flyway.validate();
    }
}
