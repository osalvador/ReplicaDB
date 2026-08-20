package org.replicadb.server.job.persistence;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class FlywayMigrationTest {

    @Container
        static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    @Test
    void appliesAndValidatesMetadataMigrations() throws Exception {
        Flyway initialFlyway = flyway().target("12").load();
        initialFlyway.migrate();

        UUID completeId = UUID.randomUUID();
        UUID incrementalId = UUID.randomUUID();
        UUID atomicId = UUID.randomUUID();
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO job_definition
                         (id, name, source_connect, source_table, sink_connect, sink_table, mode, jobs)
                     VALUES (?, ?, 'jdbc:source', 'source_table', 'jdbc:sink', 'sink_table', ?, 1)
                     """)) {
            insertLegacyDefinition(statement, completeId, "legacy-complete", "complete");
            insertLegacyDefinition(statement, incrementalId, "legacy-incremental", "incremental");
            insertLegacyDefinition(statement, atomicId, "legacy-atomic", "complete-atomic");
        }

        Flyway flyway = flyway().target("13").load();
        assertEquals(1, flyway.migrate().migrationsExecuted);
        assertEquals(13, flyway.info().applied().length);
        flyway.validate();

        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT max_attempts, retry_backoff_seconds, automatic_retry_enabled
                     FROM job_definition WHERE id = ?
                     """)) {
            assertPolicy(statement, completeId, false);
            assertPolicy(statement, incrementalId, true);
            assertPolicy(statement, atomicId, true);
        }

        assertTrue(hasConstraint("ck_job_definition_max_attempts"));
        assertTrue(hasConstraint("ck_job_definition_retry_backoff_seconds"));
        assertFalse(hasNullablePolicyColumn("max_attempts"));
        assertFalse(hasNullablePolicyColumn("retry_backoff_seconds"));
        assertFalse(hasNullablePolicyColumn("automatic_retry_enabled"));

        UUID pendingRunId = UUID.randomUUID();
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
            POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement("""
                 INSERT INTO job_run (id, job_definition_id, status, attempt, created_at)
                 VALUES (?, ?, 'PENDING', 1, now() - interval '1 second')
                 """)) {
            statement.setObject(1, pendingRunId);
            statement.setObject(2, completeId);
            statement.executeUpdate();
        }

        Flyway leaseFlyway = flyway().load();
        assertEquals(1, leaseFlyway.migrate().migrationsExecuted);
        assertEquals(14, leaseFlyway.info().applied().length);
        leaseFlyway.validate();

        assertTrue(hasIndex("idx_job_run_eligible"));
        assertFalse(hasIndex("idx_job_run_pending"));
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
            POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT available_at IS NOT NULL AND available_at <= now(), lease_token
                 FROM job_run WHERE id = ?
                 """)) {
            statement.setObject(1, pendingRunId);
            try (ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            assertTrue(resultSet.getBoolean(1));
            assertEquals(null, resultSet.getObject("lease_token"));
            }
        }

        UUID replacementRunId = UUID.randomUUID();
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
            POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE job_run SET status = 'RETRY_SCHEDULED' WHERE id = '"
                + pendingRunId + "'");
            try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO job_run (id, job_definition_id, previous_run_id, status, attempt)
                VALUES (?, ?, ?, 'PENDING', 2)
                """)) {
            insert.setObject(1, replacementRunId);
            insert.setObject(2, completeId);
            insert.setObject(3, pendingRunId);
            insert.executeUpdate();
            }
        }

        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
            POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                 "SELECT COUNT(*) FROM job_run WHERE job_definition_id = ?")) {
            statement.setObject(1, completeId);
            try (ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            }
        }
    }

    private static FluentConfiguration flyway() {
        return Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void insertLegacyDefinition(PreparedStatement statement, UUID id,
                                               String name, String mode) throws Exception {
        statement.setObject(1, id);
        statement.setString(2, name);
        statement.setString(3, mode);
        statement.executeUpdate();
    }

    private static void assertPolicy(PreparedStatement statement, UUID id,
                                     boolean automaticRetryEnabled) throws Exception {
        statement.setObject(1, id);
        try (ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            assertEquals(3, resultSet.getInt("max_attempts"));
            assertEquals(60, resultSet.getLong("retry_backoff_seconds"));
            assertEquals(automaticRetryEnabled, resultSet.getBoolean("automatic_retry_enabled"));
        }
    }

    private static boolean hasConstraint(String constraintName) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = ?)");) {
            statement.setString(1, constraintName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    private static boolean hasNullablePolicyColumn(String columnName) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT is_nullable = 'YES'
                     FROM information_schema.columns
                     WHERE table_name = 'job_definition' AND column_name = ?
                     """)) {
            statement.setString(1, columnName);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getBoolean(1);
            }
        }
    }

    private static boolean hasIndex(String indexName) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = ?)");) {
            statement.setString(1, indexName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }
}
