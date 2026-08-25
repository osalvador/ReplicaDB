package org.replicadb.server.observability;

import org.replicadb.server.job.port.JobRunStore;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public final class RunQueueHealthIndicator implements HealthIndicator {

    private static final int SNAPSHOT_LIMIT = 100;

    private final JobRunStore jobRunStore;

    public RunQueueHealthIndicator(JobRunStore jobRunStore) {
        this.jobRunStore = jobRunStore;
    }

    @Override
    public Health health() {
        try {
            JobRunStore.EligibleRunSnapshot snapshot = jobRunStore.findEligibleRunSnapshot(SNAPSHOT_LIMIT);
            Health.Builder health = Health.up()
                    .withDetail("eligibleCount", snapshot.eligibleCount())
                    .withDetail("countTruncated", snapshot.truncated());
            Instant oldest = snapshot.oldestAvailableAt();
            if (oldest != null) {
                health.withDetail("oldestAvailableAt", oldest.toString());
            }
            return health.build();
        } catch (RuntimeException ignored) {
            return Health.down().withDetail("queue", "unavailable").build();
        }
    }
}
