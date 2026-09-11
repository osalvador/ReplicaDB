package org.replicadb.server.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Profile("api")
public final class QuartzHealthIndicator implements HealthIndicator {

    private static final String CHECKIN_AGE = "replicadb.managed.scheduler.checkin.age";
    private static final String CHECKIN_STATE_SQL = "SELECT LAST_CHECKIN_TIME, CHECKIN_INTERVAL "
            + "FROM QRTZ_SCHEDULER_STATE WHERE SCHED_NAME = ? AND INSTANCE_NAME = ?";

    private final Scheduler scheduler;
    private final DataSource dataSource;
    private final int checkinStaleFactor;
    private final AtomicLong lastCheckinAgeMs = new AtomicLong(-1);

    public QuartzHealthIndicator(Scheduler scheduler,
                                 DataSource dataSource,
                                 MeterRegistry meterRegistry,
                                 @Value("${replicadb.server.scheduler.checkin-stale-factor:3}") int checkinStaleFactor) {
        this.scheduler = scheduler;
        this.dataSource = dataSource;
        this.checkinStaleFactor = checkinStaleFactor;
        meterRegistry.gauge(CHECKIN_AGE, lastCheckinAgeMs, AtomicLong::get);
    }

    @Override
    public Health health() {
        try {
            if (!scheduler.isStarted() || scheduler.isShutdown()) {
                return Health.down()
                        .withDetail("scheduler", "unavailable")
                        .withDetail("lastCheckinAgeMs", -1)
                        .build();
            }

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(CHECKIN_STATE_SQL)) {
                statement.setQueryTimeout(2);
                statement.setString(1, scheduler.getSchedulerName());
                statement.setString(2, scheduler.getSchedulerInstanceId());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        lastCheckinAgeMs.set(-1);
                        return Health.up()
                                .withDetail("scheduler", "starting")
                                .withDetail("lastCheckinAgeMs", -1)
                                .build();
                    }

                    long checkinAgeMs = Math.max(0, System.currentTimeMillis() - resultSet.getLong("LAST_CHECKIN_TIME"));
                    long checkinIntervalMs = resultSet.getLong("CHECKIN_INTERVAL");
                    lastCheckinAgeMs.set(checkinAgeMs);
                    if (checkinIntervalMs <= 0
                            || checkinAgeMs > checkinIntervalMs * Math.max(1, checkinStaleFactor)) {
                        return Health.down()
                                .withDetail("scheduler", "stale-checkin")
                                .withDetail("lastCheckinAgeMs", checkinAgeMs)
                                .build();
                    }
                    return Health.up()
                            .withDetail("scheduler", "running")
                            .withDetail("lastCheckinAgeMs", checkinAgeMs)
                            .build();
                }
            }
        } catch (SQLException | SchedulerException ignored) {
            lastCheckinAgeMs.set(-1);
        }
        return Health.down()
            .withDetail("scheduler", "unavailable")
            .withDetail("lastCheckinAgeMs", -1)
            .build();
    }
}
