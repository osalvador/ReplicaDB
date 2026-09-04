package org.replicadb.server.job.persistence;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.FlywayException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class FlywayMigrationTest {

    @Container
        static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    @Test
    void appliesAndValidatesMetadataMigrations() throws Exception {
        resetPublicSchema();
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

        Flyway leaseFlyway = flyway().target("16").load();
        assertEquals(3, leaseFlyway.migrate().migrationsExecuted);
        assertEquals(16, leaseFlyway.info().applied().length);
        assertEquals(0, leaseFlyway.info().pending().length);
        leaseFlyway.validate();

        assertTrue(hasIndex("idx_job_run_eligible"));
        assertFalse(hasIndex("idx_job_run_pending"));
        assertTrue(hasTable("qrtz_job_details"));
        assertTrue(hasTable("qrtz_triggers"));
        assertTrue(hasTable("qrtz_fired_triggers"));
        assertTrue(hasTable("qrtz_scheduler_state"));
        assertTrue(hasTable("qrtz_locks"));
        assertTrue(hasIndex("idx_qrtz_t_nft_st_misfire_grp"));
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

    @Test
    void appliesDatasourceMigrationsOnAnEmptyManagedSchema() throws Exception {
        String schema = isolatedSchema();
        createSchema(schema);
        try {
            Flyway flyway = flyway(schema).load();
            assertEquals(21, flyway.migrate().migrationsExecuted);
            assertEquals(21, flyway.info().applied().length);
            assertEquals(0, flyway.info().pending().length);
            flyway.validate();

            assertTrue(hasTable(schema, "managed_datasource"));
            assertTrue(hasTable(schema, "datasource_permission"));
            assertTrue(hasColumn(schema, "managed_datasource", "encrypted_security"));
            assertTrue(hasColumn(schema, "managed_datasource", "technical_params"));
            assertTrue(hasColumn(schema, "job_definition", "source_datasource_id"));
            assertTrue(hasColumn(schema, "job_definition", "sink_datasource_id"));
            assertTrue(hasColumn(schema, "job_definition", "source_datasource_use_enabled"));
            assertTrue(hasColumn(schema, "job_definition", "sink_datasource_use_enabled"));
            assertTrue(hasColumn(schema, "job_run", "resolved_source_datasource_id"));
            assertTrue(hasColumn(schema, "job_run", "resolved_sink_datasource_id"));
            assertTrue(hasColumn(schema, "job_run", "datasources_resolved_at"));
            assertFalse(hasColumn(schema, "job_definition", "source_connect"));
            assertFalse(hasColumn(schema, "job_definition", "sink_connect"));
            assertTrue(hasIndex(schema, "idx_job_definition_source_datasource"));
            assertTrue(hasIndex(schema, "idx_job_definition_sink_datasource"));
            assertTrue(hasIndex(schema, "idx_job_run_resolved_source_datasource"));
            assertTrue(hasIndex(schema, "idx_job_run_resolved_sink_datasource"));
            assertTrue(hasTable(schema, "run_log"));
            assertTrue(hasColumn(schema, "run_log", "captured_size"));
            assertTrue(hasColumn(schema, "run_log", "format_version"));
                assertForeignKeyDeleteRule(schema, "job_run", "fk_job_run_job_definition", "c");
                assertForeignKeyDeleteRule(schema, "run_trigger_idempotency",
                    "fk_run_trigger_idempotency_job_definition", "c");

            UUID datasourceId = UUID.randomUUID();
            UUID jobId = UUID.randomUUID();
            try (Connection connection = connection(schema);
                 PreparedStatement datasource = connection.prepareStatement("""
                         INSERT INTO managed_datasource
                             (id, name, connector_type, safe_connect_display, technical_params,
                              encrypted_security, security_format_version, encryption_algorithm, key_version)
                         VALUES (?, 'source', 'postgres', 'jdbc:postgresql://[REDACTED]', '{}'::jsonb,
                                 ?, 1, 'AES/GCM/NoPadding', 'test')
                         """)) {
                datasource.setObject(1, datasourceId);
                datasource.setBytes(2, new byte[]{1});
                datasource.executeUpdate();
            }

            try (Connection connection = connection(schema);
                 PreparedStatement job = connection.prepareStatement("""
                         INSERT INTO job_definition
                             (id, name, source_datasource_id, sink_datasource_id,
                              source_table, sink_table, mode, jobs)
                         VALUES (?, 'bound-job', ?, ?, 'source_table', 'sink_table', 'complete', 1)
                         """)) {
                job.setObject(1, jobId);
                job.setObject(2, datasourceId);
                job.setObject(3, datasourceId);
                job.executeUpdate();
            }

            try (Connection connection = connection(schema);
                 PreparedStatement delete = connection.prepareStatement(
                         "DELETE FROM managed_datasource WHERE id = ?")) {
                delete.setObject(1, datasourceId);
                assertThrows(java.sql.SQLException.class, delete::executeUpdate);
            }
        } finally {
            dropSchema(schema);
        }
    }

    @Test
    void cascadesAllJobOwnedStateButKeepsAuditHistory() throws Exception {
        String schema = isolatedSchema();
        createSchema(schema);
        try {
            Flyway flyway = flyway(schema).load();
            flyway.migrate();

            UUID datasourceId = UUID.randomUUID();
            UUID jobId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            UUID firstRunId = UUID.randomUUID();
            UUID secondRunId = UUID.randomUUID();
            UUID thirdRunId = UUID.randomUUID();
            try (Connection connection = connection(schema)) {
                insertDatasource(connection, datasourceId);
                try (PreparedStatement user = connection.prepareStatement("""
                        INSERT INTO app_user (id, username, password_hash, role, enabled)
                        VALUES (?, 'delete-user', 'hash', 'VIEWER', true)
                        """)) {
                    user.setObject(1, userId);
                    user.executeUpdate();
                }
                try (PreparedStatement job = connection.prepareStatement("""
                        INSERT INTO job_definition
                            (id, name, source_datasource_id, sink_datasource_id,
                             source_table, sink_table, mode, jobs)
                        VALUES (?, 'delete-job', ?, ?, 'source_table', 'sink_table', 'complete', 1)
                        """)) {
                    job.setObject(1, jobId);
                    job.setObject(2, datasourceId);
                    job.setObject(3, datasourceId);
                    job.executeUpdate();
                }
                try (PreparedStatement schedule = connection.prepareStatement("""
                        INSERT INTO job_schedule (job_definition_id, cron_expression, time_zone)
                        VALUES (?, '* * * * * ?', 'UTC')
                        """)) {
                    schedule.setObject(1, jobId);
                    schedule.executeUpdate();
                }
                try (PreparedStatement permission = connection.prepareStatement("""
                        INSERT INTO job_permission (job_definition_id, user_id, permission)
                        VALUES (?, ?, 'VIEW')
                        """)) {
                    permission.setObject(1, jobId);
                    permission.setObject(2, userId);
                    permission.executeUpdate();
                }
                insertRun(connection, firstRunId, jobId, null, "SUCCEEDED");
                insertRun(connection, secondRunId, jobId, firstRunId, "RETRY_SCHEDULED");
                insertRun(connection, thirdRunId, jobId, secondRunId, "CANCELLED");
                try (PreparedStatement log = connection.prepareStatement("""
                        INSERT INTO run_log
                            (run_id, content, captured_size, format_version, captured_at, updated_at)
                        VALUES (?, 'log', 3, 1, now(), now())
                        """)) {
                    log.setObject(1, thirdRunId);
                    log.executeUpdate();
                }
                try (PreparedStatement idempotency = connection.prepareStatement("""
                        INSERT INTO run_trigger_idempotency (idempotency_key, job_definition_id, run_id)
                        VALUES ('delete-key', ?, ?)
                        """)) {
                    idempotency.setObject(1, jobId);
                    idempotency.setObject(2, thirdRunId);
                    idempotency.executeUpdate();
                }
                try (PreparedStatement audit = connection.prepareStatement("""
                        INSERT INTO audit_event
                            (id, actor_username, action, resource_type, resource_id, outcome)
                        VALUES (?, 'admin', 'JOB_CREATED', 'JOB_DEFINITION', ?, 'SUCCESS')
                        """)) {
                    audit.setObject(1, UUID.randomUUID());
                    audit.setString(2, jobId.toString());
                    audit.executeUpdate();
                }
                try (PreparedStatement delete = connection.prepareStatement(
                        "DELETE FROM job_definition WHERE id = ?")) {
                    delete.setObject(1, jobId);
                    assertEquals(1, delete.executeUpdate());
                }
            }

            try (Connection connection = connection(schema)) {
                assertCount(connection, "job_definition", "id", jobId, 0);
                assertCount(connection, "job_schedule", "job_definition_id", jobId, 0);
                assertCount(connection, "job_permission", "job_definition_id", jobId, 0);
                assertCount(connection, "job_run", "job_definition_id", jobId, 0);
                assertCount(connection, "run_log", "run_id", thirdRunId, 0);
                assertCount(connection, "run_trigger_idempotency", "job_definition_id", jobId, 0);
                assertCount(connection, "audit_event", "resource_id", jobId.toString(), 1);
            }
        } finally {
            dropSchema(schema);
        }
    }

    @Test
    void rejectsV21WhenIdempotencyContainsAnOrphanedJobReference() throws Exception {
        String schema = isolatedSchema();
        createSchema(schema);
        try {
            Flyway initial = flyway(schema).target("20").load();
            initial.migrate();
            try (Connection connection = connection(schema);
                 PreparedStatement statement = connection.prepareStatement("""
                         INSERT INTO run_trigger_idempotency (idempotency_key, job_definition_id, run_id)
                         VALUES ('orphan-key', ?, ?)
                         """)) {
                statement.setObject(1, UUID.randomUUID());
                statement.setObject(2, UUID.randomUUID());
                statement.executeUpdate();
            }

            FlywayException exception = assertThrows(FlywayException.class, () -> flyway(schema).load().migrate());
            assertTrue(exception.getMessage().contains("orphaned job references"));
        } finally {
            dropSchema(schema);
        }
    }

    @Test
    void rejectsDatasourceMigrationWhenLegacyManagedStateExists() throws Exception {
        String schema = isolatedSchema();
        createSchema(schema);
        try {
            Flyway initial = flyway(schema).target("16").load();
            initial.migrate();

            UUID legacyId = UUID.randomUUID();
            try (Connection connection = connection(schema);
                 PreparedStatement statement = connection.prepareStatement("""
                         INSERT INTO job_definition
                             (id, name, source_connect, source_table, sink_connect, sink_table, mode, jobs)
                         VALUES (?, 'legacy-job', 'jdbc:source', 'source_table', 'jdbc:sink', 'sink_table', 'complete', 1)
                         """)) {
                statement.setObject(1, legacyId);
                statement.executeUpdate();
            }

            Flyway phase4 = flyway(schema).load();
            FlywayException exception = assertThrows(FlywayException.class, phase4::migrate);
            assertTrue(exception.getMessage().contains("empty managed metadata state"));
            assertTrue(hasTable(schema, "managed_datasource"));
            assertTrue(hasColumn(schema, "job_definition", "source_connect"));
            assertFalse(hasColumn(schema, "job_definition", "source_datasource_id"));
        } finally {
            dropSchema(schema);
        }
    }

    private static FluentConfiguration flyway() {
        return Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration");
    }

    private static FluentConfiguration flyway(String schema) {
        return flyway().schemas(schema).defaultSchema(schema);
    }

    private static String isolatedSchema() {
        return "replicadb_phase4_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static void createSchema(String schema) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }

    private static void dropSchema(String schema) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    private static void resetPublicSchema() throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA public CASCADE");
            statement.execute("CREATE SCHEMA public");
        }
    }

    private static Connection connection(String schema) throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl() + "&currentSchema=" + schema,
                POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void insertLegacyDefinition(PreparedStatement statement, UUID id,
                                               String name, String mode) throws Exception {
        statement.setObject(1, id);
        statement.setString(2, name);
        statement.setString(3, mode);
        statement.executeUpdate();
    }

    private static void insertDatasource(Connection connection, UUID id) throws Exception {
        try (PreparedStatement datasource = connection.prepareStatement("""
                INSERT INTO managed_datasource
                    (id, name, connector_type, safe_connect_display, technical_params,
                     encrypted_security, security_format_version, encryption_algorithm, key_version)
                VALUES (?, 'delete-datasource', 'postgres', 'jdbc:postgresql://[REDACTED]', '{}'::jsonb,
                        ?, 1, 'AES/GCM/NoPadding', 'test')
                """)) {
            datasource.setObject(1, id);
            datasource.setBytes(2, new byte[]{1});
            datasource.executeUpdate();
        }
    }

    private static void insertRun(Connection connection, UUID id, UUID jobId, UUID previousRunId,
                                  String status) throws Exception {
        try (PreparedStatement run = connection.prepareStatement("""
                INSERT INTO job_run (id, job_definition_id, previous_run_id, status, attempt)
                VALUES (?, ?, ?, ?, 1)
                """)) {
            run.setObject(1, id);
            run.setObject(2, jobId);
            run.setObject(3, previousRunId);
            run.setString(4, status);
            run.executeUpdate();
        }
    }

    private static void assertCount(Connection connection, String table, String column,
                                     Object value, int expected) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COUNT(*) FROM " + table + " WHERE " + column + " = ?")) {
            statement.setObject(1, value);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                assertEquals(expected, resultSet.getInt(1));
            }
        }
    }

    private static void assertForeignKeyDeleteRule(String schema, String table, String constraint,
                                                    String expectedDeleteRule) throws Exception {
        try (Connection connection = connection(schema);
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT c.confdeltype
                     FROM pg_constraint c
                     JOIN pg_class table_ref ON table_ref.oid = c.conrelid
                     JOIN pg_namespace namespace_ref ON namespace_ref.oid = table_ref.relnamespace
                     WHERE namespace_ref.nspname = ? AND table_ref.relname = ? AND c.conname = ?
                     """)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            statement.setString(3, constraint);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                assertEquals(expectedDeleteRule, resultSet.getString(1));
            }
        }
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

    private static boolean hasIndex(String schema, String indexName) throws Exception {
        try (Connection connection = connection(schema);
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = ? AND indexname = ?)")) {
            statement.setString(1, schema);
            statement.setString(2, indexName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    private static boolean hasTable(String tableName) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    private static boolean hasTable(String schema, String tableName) throws Exception {
        try (Connection connection = connection(schema);
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT EXISTS (
                         SELECT 1 FROM information_schema.tables
                         WHERE table_schema = ? AND table_name = ?
                     )
                     """)) {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    private static boolean hasColumn(String schema, String tableName, String columnName) throws Exception {
        try (Connection connection = connection(schema);
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT EXISTS (
                         SELECT 1 FROM information_schema.columns
                         WHERE table_schema = ? AND table_name = ? AND column_name = ?
                     )
                     """)) {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            statement.setString(3, columnName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }
}
