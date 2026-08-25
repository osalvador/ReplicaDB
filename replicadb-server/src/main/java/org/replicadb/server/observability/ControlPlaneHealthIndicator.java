package org.replicadb.server.observability;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Component
public final class ControlPlaneHealthIndicator implements HealthIndicator {

    private final DataSource dataSource;

    public ControlPlaneHealthIndicator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Health health() {
        try (Connection connection = dataSource.getConnection()) {
            if (connection.isValid(2)) {
                return Health.up().withDetail("database", "reachable").build();
            }
        } catch (SQLException | RuntimeException ignored) {
            // Health output must not expose driver or connection details.
        }
        return Health.down().withDetail("database", "unavailable").build();
    }
}
