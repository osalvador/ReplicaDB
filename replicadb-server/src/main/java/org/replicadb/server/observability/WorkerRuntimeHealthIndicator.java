package org.replicadb.server.observability;

import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("worker")
public final class WorkerRuntimeHealthIndicator implements HealthIndicator {

    public static final Status DEGRADED = new Status("DEGRADED");

    private final WorkerDispatchCoordinator coordinator;
    private final PostgreSQLNotificationListener notificationListener;
    private final PollingFallback pollingFallback;

    public WorkerRuntimeHealthIndicator(WorkerDispatchCoordinator coordinator,
                                        PostgreSQLNotificationListener notificationListener,
                                        PollingFallback pollingFallback) {
        this.coordinator = coordinator;
        this.notificationListener = notificationListener;
        this.pollingFallback = pollingFallback;
    }

    @Override
    public Health health() {
        boolean accepting = coordinator.isAccepting();
        boolean polling = pollingFallback.isRunning();
        boolean listener = notificationListener.isConnected();
        Health.Builder health = accepting && polling ? Health.status(listener ? Status.UP : DEGRADED) : Health.down();
        return health.withDetail("accepting", accepting)
                .withDetail("polling", polling)
                .withDetail("listenerConnected", listener)
                .withDetail("activeSlots", coordinator.maxConcurrentRuns() - coordinator.availableCapacity())
                .withDetail("freeSlots", coordinator.availableCapacity())
                .build();
    }
}
