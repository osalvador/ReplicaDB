package org.replicadb.server.config;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

@TestConfiguration(proxyBeanMethods = false)
public class PostgresTestcontainersConfig {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        return new PostgreSQLContainer<>("postgres:16-alpine")
                .waitingFor(Wait.forListeningPort());
    }

    public static String isolatedSchema() {
        return "replicadb_test_" + UUID.randomUUID().toString().replace("-", "");
    }

    public static String jdbcUrl(PostgreSQLContainer<?> container, String schema) {
        return container.getJdbcUrl() + "&currentSchema=" + schema;
    }

    public static void migrate(PostgreSQLContainer<?> container, String schema) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        FluentConfiguration configuration = Flyway.configure()
                .dataSource(container.getJdbcUrl(), container.getUsername(), container.getPassword())
                .schemas(schema)
                .defaultSchema(schema)
                .locations("classpath:db/migration");
        configuration.load().migrate();
    }

    public static void dropSchema(PostgreSQLContainer<?> container, String schema) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }
}
