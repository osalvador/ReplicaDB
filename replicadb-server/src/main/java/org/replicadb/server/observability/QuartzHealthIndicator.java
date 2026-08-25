package org.replicadb.server.observability;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("api")
public final class QuartzHealthIndicator implements HealthIndicator {

    private final Scheduler scheduler;

    public QuartzHealthIndicator(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Health health() {
        try {
            if (scheduler.isStarted() && !scheduler.isShutdown()) {
                return Health.up().withDetail("scheduler", "running").build();
            }
        } catch (SchedulerException ignored) {
            // Return a fixed failure detail rather than Quartz internals.
        }
        return Health.down().withDetail("scheduler", "unavailable").build();
    }
}
