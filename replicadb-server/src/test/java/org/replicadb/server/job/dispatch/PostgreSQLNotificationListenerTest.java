package org.replicadb.server.job.dispatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.port.RunNotificationPublisher;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostgreSQLNotificationListenerTest {

    private final List<PostgreSQLNotificationListener> listeners = new ArrayList<>();
    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach
    void stopListeners() {
        listeners.forEach(PostgreSQLNotificationListener::stop);
        executors.forEach(ExecutorService::shutdownNow);
    }

    @Test
    void parserAcceptsOnlyUuidPayloads() {
        NotificationPayloadParser parser = new NotificationPayloadParser();
        UUID runId = UUID.randomUUID();

        assertEquals(Optional.of(runId), parser.parse(runId.toString()));
        assertEquals(Optional.of(runId), parser.parse("  " + runId + "  "));
        assertTrue(parser.parse(null).isEmpty());
        assertTrue(parser.parse("").isEmpty());
        assertTrue(parser.parse("jdbc:postgresql://user:password@host/db").isEmpty());
    }

    @Test
    void routesValidChannelsAndIgnoresMalformedPayloads() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PGConnection pgConnection = mock(PGConnection.class);
        Connection connection = connection(pgConnection);
        UUID runId = UUID.randomUUID();
        UUID cancellationId = UUID.randomUUID();
        PGNotification validRun = notification(RunNotificationPublisher.RUN_CHANNEL, runId.toString());
        PGNotification malformedRun = notification(RunNotificationPublisher.RUN_CHANNEL, "not-a-uuid");
        PGNotification validCancellation = notification(RunNotificationPublisher.CONTROL_CHANNEL,
                cancellationId.toString());
        PGNotification malformedCancellation = notification(RunNotificationPublisher.CONTROL_CHANNEL,
                "jdbc:postgresql://user:password@host/db");
        AtomicBoolean delivered = new AtomicBoolean();
        CountDownLatch routed = new CountDownLatch(2);
        when(pgConnection.getNotifications(anyInt())).thenAnswer(invocation -> {
            if (delivered.compareAndSet(false, true)) {
                return new PGNotification[]{validRun, malformedRun, validCancellation, malformedCancellation};
            }
            return new PGNotification[0];
        });
        doAnswer(invocation -> {
            routed.countDown();
            return null;
        }).when(coordinator).signalRun(eq(runId), anyLong());
        when(coordinator.signalCancellation(cancellationId)).thenAnswer(invocation -> {
            routed.countDown();
            return true;
        });
        PostgreSQLNotificationListener listener = listener(
                () -> connection, coordinator, polling,
                Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(10), Duration.ofSeconds(1),
                ignored -> { });

        listener.start();

        assertTrue(routed.await(2, TimeUnit.SECONDS));
        listener.stop();
        verify(coordinator).signalRun(eq(runId), anyLong());
        verify(coordinator).signalCancellation(cancellationId);
        verify(polling).onListenerReconnected();
        verify(connection.createStatement()).execute("LISTEN " + RunNotificationPublisher.RUN_CHANNEL);
        verify(connection.createStatement()).execute("LISTEN " + RunNotificationPublisher.CONTROL_CHANNEL);
    }

    @Test
    void reconnectsAfterFailureResubscribesAndWakesPolling() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PGConnection failedPgConnection = mock(PGConnection.class);
        PGConnection recoveredPgConnection = mock(PGConnection.class);
        Connection failedConnection = connection(failedPgConnection);
        Connection recoveredConnection = connection(recoveredPgConnection);
        UUID runId = UUID.randomUUID();
        CountDownLatch routed = new CountDownLatch(1);
        when(failedPgConnection.getNotifications(anyInt())).thenThrow(new SQLException("connection lost"));
        AtomicBoolean delivered = new AtomicBoolean();
        when(recoveredPgConnection.getNotifications(anyInt())).thenAnswer(invocation -> {
            if (delivered.compareAndSet(false, true)) {
                return new PGNotification[]{notification(RunNotificationPublisher.RUN_CHANNEL, runId.toString())};
            }
            return new PGNotification[0];
        });
        doAnswer(invocation -> {
            routed.countDown();
            return null;
        }).when(coordinator).signalRun(eq(runId), anyLong());
        AtomicInteger connections = new AtomicInteger();
        List<Duration> delays = new ArrayList<>();
        PostgreSQLNotificationListener listener = listener(
            () -> connections.incrementAndGet() == 1 ? failedConnection : recoveredConnection,
                coordinator, polling, Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(10),
                Duration.ofSeconds(1), delays::add);

        listener.start();

        assertTrue(routed.await(2, TimeUnit.SECONDS));
        listener.stop();
        assertEquals(2, connections.get());
        assertEquals(List.of(Duration.ofMillis(10)), delays);
        verify(polling, times(2)).onListenerReconnected();
        verify(failedConnection.createStatement(), times(1)).execute("LISTEN " + RunNotificationPublisher.RUN_CHANNEL);
        verify(recoveredConnection.createStatement(), times(1))
                .execute("LISTEN " + RunNotificationPublisher.CONTROL_CHANNEL);
    }

    @Test
    void exponentialReconnectBackoffIsCapped() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        CountDownLatch fourthDelay = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Duration> delays = new ArrayList<>();
        PostgreSQLNotificationListener.Sleeper sleeper = delay -> {
            delays.add(delay);
            if (delays.size() == 4) {
                fourthDelay.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        PostgreSQLNotificationListener listener = listener(
            () -> { throw new SQLException("database unavailable"); }, coordinator, polling,
                Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(10), Duration.ofSeconds(1), sleeper);

        listener.start();

        assertTrue(fourthDelay.await(2, TimeUnit.SECONDS));
        listener.stop();
        release.countDown();
        assertEquals(List.of(Duration.ofMillis(10), Duration.ofMillis(20),
                Duration.ofMillis(40), Duration.ofMillis(40)), delays);
    }

    @Test
    void successfulSubscriptionResetsReconnectDelay() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PGConnection recoveredPgConnection = mock(PGConnection.class);
        Connection recoveredConnection = connection(recoveredPgConnection);
        AtomicInteger opens = new AtomicInteger();
        List<Duration> delays = new ArrayList<>();
        CountDownLatch resetObserved = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        PostgreSQLNotificationListener.Sleeper sleeper = delay -> {
            delays.add(delay);
            if (delays.size() == 2) {
                resetObserved.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        when(recoveredPgConnection.getNotifications(anyInt())).thenThrow(new SQLException("connection lost"));
        PostgreSQLNotificationListener listener = listener(
                () -> {
                    if (opens.incrementAndGet() == 1) {
                        throw new SQLException("initial failure");
                    }
                    return recoveredConnection;
                },
                coordinator, polling, Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(10),
                Duration.ofSeconds(1), sleeper);

        listener.start();

        assertTrue(resetObserved.await(2, TimeUnit.SECONDS));
        listener.stop();
        release.countDown();
        assertEquals(List.of(Duration.ofMillis(10), Duration.ofMillis(10)), delays);
        verify(polling).onListenerReconnected();
    }

    @Test
    void shutdownClosesTheDedicatedConnectionWhileNotificationWaitIsBlocked() throws Exception {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PGConnection pgConnection = mock(PGConnection.class);
        Connection connection = connection(pgConnection);
        CountDownLatch entered = new CountDownLatch(1);
        when(pgConnection.getNotifications(anyInt())).thenAnswer(invocation -> {
            entered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            return null;
        });
        PostgreSQLNotificationListener listener = listener(
            () -> connection, coordinator, polling, Duration.ofMillis(10), Duration.ofMillis(40),
                Duration.ofMillis(10), Duration.ofSeconds(1), ignored -> { });

        listener.start();

        assertTrue(entered.await(2, TimeUnit.SECONDS));
        listener.stop();
        assertFalse(listener.isRunning());
        assertTrue(listener.isShutdown());
        verify(connection, atLeastOnce()).close();
    }

    private PostgreSQLNotificationListener listener(PostgreSQLNotificationListener.ConnectionProvider provider,
                                                    WorkerDispatchCoordinator coordinator,
                                                    PollingFallback polling,
                                                    Duration initialDelay,
                                                    Duration maxDelay,
                                                    Duration notificationWait,
                                                    Duration shutdownTimeout,
                                                    PostgreSQLNotificationListener.Sleeper sleeper) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executors.add(executor);
        PostgreSQLNotificationListener listener = new PostgreSQLNotificationListener(
                provider, coordinator, polling, initialDelay, maxDelay, notificationWait,
                shutdownTimeout, sleeper, executor);
        listeners.add(listener);
        return listener;
    }

    private static Connection connection(PGConnection pgConnection) throws SQLException {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(connection.createStatement()).thenReturn(statement);
        return connection;
    }

    private static PGNotification notification(String channel, String payload) {
        PGNotification notification = mock(PGNotification.class);
        when(notification.getName()).thenReturn(channel);
        when(notification.getParameter()).thenReturn(payload);
        return notification;
    }

}
