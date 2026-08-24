package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresNotificationPublisherIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    @Test
    void deliversCommittedRunNotificationAfterCommit() throws Exception {
        DataSource dataSource = dataSource();
        PostgresNotificationPublisher publisher = new PostgresNotificationPublisher(new JdbcTemplate(dataSource));
        TransactionTemplate transaction = transactionTemplate(dataSource);
        UUID runId = UUID.randomUUID();

        try (Connection listener = listeningConnection(dataSource, RunNotificationPublisher.RUN_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            transaction.executeWithoutResult(status -> publisher.publishRun(runId));

            List<PGNotification> notifications = awaitNotifications(pgConnection, 2_000);
            assertEquals(1, notifications.size());
            assertEquals(RunNotificationPublisher.RUN_CHANNEL, notifications.get(0).getName());
            assertEquals(runId.toString(), notifications.get(0).getParameter());
        }
    }

    @Test
    void rolledBackTransactionProducesNoNotification() throws Exception {
        DataSource dataSource = dataSource();
        PostgresNotificationPublisher publisher = new PostgresNotificationPublisher(new JdbcTemplate(dataSource));
        TransactionTemplate transaction = transactionTemplate(dataSource);
        UUID runId = UUID.randomUUID();

        try (Connection listener = listeningConnection(dataSource, RunNotificationPublisher.RUN_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            transaction.executeWithoutResult(status -> {
                publisher.publishRun(runId);
                status.setRollbackOnly();
            });

            assertTrue(awaitNotifications(pgConnection, 500).stream()
                    .noneMatch(notification -> runId.toString().equals(notification.getParameter())));
        }
    }

    @Test
    void publishesOneUuidPayloadPerChannelOnTheCallerTransaction() throws Exception {
        DataSource dataSource = dataSource();
        PostgresNotificationPublisher publisher = new PostgresNotificationPublisher(new JdbcTemplate(dataSource));
        TransactionTemplate transaction = transactionTemplate(dataSource);
        UUID runId = UUID.randomUUID();
        UUID cancellationId = UUID.randomUUID();

        try (Connection listener = listeningConnection(dataSource,
                RunNotificationPublisher.RUN_CHANNEL, RunNotificationPublisher.CONTROL_CHANNEL)) {
            PGConnection pgConnection = listener.unwrap(PGConnection.class);
            transaction.executeWithoutResult(status -> {
                publisher.publishRun(runId);
                publisher.publishCancellation(cancellationId);
            });

            List<PGNotification> notifications = awaitNotifications(pgConnection, 2_000);
            assertEquals(2, notifications.size());
            assertEquals(Set.of(RunNotificationPublisher.RUN_CHANNEL,
                    RunNotificationPublisher.CONTROL_CHANNEL), notifications.stream()
                    .map(PGNotification::getName).collect(Collectors.toSet()));
            assertEquals(Set.of(runId.toString(), cancellationId.toString()), notifications.stream()
                    .map(PGNotification::getParameter).collect(Collectors.toSet()));
            assertFalse(notifications.stream().anyMatch(notification -> {
                try {
                    UUID.fromString(notification.getParameter());
                    return false;
                } catch (IllegalArgumentException exception) {
                    return true;
                }
            }));
        }
    }

    private static DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private static TransactionTemplate transactionTemplate(DataSource dataSource) {
        return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    private static Connection listeningConnection(DataSource dataSource, String... channels) throws Exception {
        Connection connection = dataSource.getConnection();
        try (Statement statement = connection.createStatement()) {
            for (String channel : channels) {
                statement.execute("LISTEN " + channel);
            }
        }
        return connection;
    }

    private static List<PGNotification> awaitNotifications(PGConnection connection, int timeoutMillis)
            throws Exception {
        PGNotification[] notifications = connection.getNotifications(timeoutMillis);
        return notifications == null ? List.of() : Arrays.asList(notifications);
    }
}