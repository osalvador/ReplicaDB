package org.replicadb.server.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.boot.actuate.health.Status;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
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
    void reportsQuartzUpWhenCheckinIsRecent() throws Exception {
        Scheduler scheduler = runningScheduler();
        ResultSet resultSet = quartzResultSet(scheduler, System.currentTimeMillis() - 1_000, 15_000);

        var health = new QuartzHealthIndicator(scheduler, resultSetDataSource(resultSet),
                new SimpleMeterRegistry(), 3).health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("running", health.getDetails().get("scheduler"));
        assertTrue(health.getDetails().containsKey("lastCheckinAgeMs"));
    }

    @Test
    void reportsQuartzDownWhenCheckinIsStale() throws Exception {
        Scheduler scheduler = runningScheduler();
        ResultSet resultSet = quartzResultSet(scheduler, System.currentTimeMillis() - 60_000, 15_000);

        var health = new QuartzHealthIndicator(scheduler, resultSetDataSource(resultSet),
                new SimpleMeterRegistry(), 3).health();

        assertEquals(Status.DOWN, health.getStatus());
        assertEquals("stale-checkin", health.getDetails().get("scheduler"));
        assertFalse(health.toString().contains("QRTZ_SCHEDULER_STATE"));
    }

    @Test
    void reportsQuartzUpDuringStartupGraceWhenNoCheckinRowYet() throws Exception {
        Scheduler scheduler = runningScheduler();
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        var health = new QuartzHealthIndicator(scheduler, dataSource, new SimpleMeterRegistry(), 3).health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("starting", health.getDetails().get("scheduler"));
        assertEquals(-1, health.getDetails().get("lastCheckinAgeMs"));
    }

    @Test
    void reportsQuartzDownWhenSchedulerNotStarted() throws Exception {
        Scheduler scheduler = mock(Scheduler.class);
        when(scheduler.isStarted()).thenReturn(false);

        var health = new QuartzHealthIndicator(scheduler, mock(DataSource.class),
                new SimpleMeterRegistry(), 3).health();

        assertEquals(Status.DOWN, health.getStatus());
        assertEquals("unavailable", health.getDetails().get("scheduler"));
    }

    @Test
    void reportsQuartzDownWhenCheckinQueryTimesOut() throws Exception {
        Scheduler scheduler = runningScheduler();
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(statement);
        doThrow(new SQLException("secret query details")).when(statement).executeQuery();

        var health = new QuartzHealthIndicator(scheduler, dataSource, new SimpleMeterRegistry(), 3).health();

        assertEquals(Status.DOWN, health.getStatus());
        assertFalse(health.toString().contains("secret query details"));
    }

    private static Scheduler runningScheduler() throws Exception {
        Scheduler scheduler = mock(Scheduler.class);
        when(scheduler.isStarted()).thenReturn(true);
        when(scheduler.isShutdown()).thenReturn(false);
        when(scheduler.getSchedulerName()).thenReturn("ReplicaDbScheduler");
        when(scheduler.getSchedulerInstanceId()).thenReturn("instance-1");
        return scheduler;
    }

    private static DataSource resultSetDataSource(ResultSet resultSet) throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        return dataSource;
    }

    private static ResultSet quartzResultSet(Scheduler scheduler, long lastCheckinTime, long checkinInterval)
            throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("LAST_CHECKIN_TIME")).thenReturn(lastCheckinTime);
        when(resultSet.getLong("CHECKIN_INTERVAL")).thenReturn(checkinInterval);
        return resultSet;
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
