package org.replicadb.server.job.persistence;

import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.UUID;

@Repository
public class PostgresNotificationPublisher implements RunNotificationPublisher {

    private static final String NOTIFY_SQL = "SELECT pg_notify(?, ?)";

    private final JdbcTemplate jdbcTemplate;

    public PostgresNotificationPublisher(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    @Override
    public void publishRun(UUID runId) {
        publish(RUN_CHANNEL, runId);
    }

    @Override
    public void publishCancellation(UUID runId) {
        publish(CONTROL_CHANNEL, runId);
    }

    private void publish(String channel, UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            try (PreparedStatement statement = connection.prepareStatement(NOTIFY_SQL)) {
                statement.setString(1, channel);
                statement.setString(2, runId.toString());
                statement.execute();
            }
            return null;
        });
    }
}