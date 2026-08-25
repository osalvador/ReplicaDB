package org.replicadb.server.observability;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.boot.actuate.health.Status;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HealthIndicatorTest {

    @Test
    void reportsDatabaseDownWithoutLeakingDriverDetails() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenThrow(new SQLException("secret-dsn"));

        var health = new ControlPlaneHealthIndicator(dataSource).health();

        assertEquals(Status.DOWN, health.getStatus());
        assertEquals("unavailable", health.getDetails().get("database"));
        assertFalse(health.toString().contains("secret-dsn"));
    }

    @Test
    void reportsDatabaseUpWhenConnectionIsValid() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.isValid(2)).thenReturn(true);

        assertEquals(Status.UP, new ControlPlaneHealthIndicator(dataSource).health().getStatus());
    }

    @Test
    void reportsQueueSnapshotWithBoundedSafeDetails() {
        JobRunStore store = mock(JobRunStore.class);
        when(store.findEligibleRunSnapshot(100)).thenReturn(
                new JobRunStore.EligibleRunSnapshot(100, true, Instant.parse("2026-08-24T10:00:00Z")));

        var health = new RunQueueHealthIndicator(store).health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals(100, health.getDetails().get("eligibleCount"));
        assertEquals(true, health.getDetails().get("countTruncated"));
        assertFalse(health.toString().contains("leaseToken"));
    }

    @Test
    void reportsWorkerDegradedWhenListenerIsDisconnectedButPollingRuns() {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PostgreSQLNotificationListener listener = mock(PostgreSQLNotificationListener.class);
        PollingFallback polling = mock(PollingFallback.class);
        when(coordinator.isAccepting()).thenReturn(true);
        when(coordinator.maxConcurrentRuns()).thenReturn(2);
        when(coordinator.availableCapacity()).thenReturn(1);
        when(listener.isConnected()).thenReturn(false);
        when(polling.isRunning()).thenReturn(true);

        var health = new WorkerRuntimeHealthIndicator(coordinator, listener, polling).health();

        assertEquals(WorkerRuntimeHealthIndicator.DEGRADED, health.getStatus());
        assertEquals(false, health.getDetails().get("listenerConnected"));
        assertEquals(1, health.getDetails().get("activeSlots"));
    }

    @Test
    void reportsWorkerDownWhenPollingStops() {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PostgreSQLNotificationListener listener = mock(PostgreSQLNotificationListener.class);
        PollingFallback polling = mock(PollingFallback.class);
        when(coordinator.isAccepting()).thenReturn(true);
        when(listener.isConnected()).thenReturn(true);
        when(polling.isRunning()).thenReturn(false);

        assertEquals(Status.DOWN,
                new WorkerRuntimeHealthIndicator(coordinator, listener, polling).health().getStatus());
    }

}
