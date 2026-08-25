package org.replicadb.server.job.dispatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.replicadb.server.observability.ManagedRuntimeMetrics;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class PostgreSQLNotificationListener implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(PostgreSQLNotificationListener.class);
    private static final Duration DEFAULT_NOTIFICATION_WAIT = Duration.ofSeconds(1);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final ConnectionProvider connectionProvider;
    private final WorkerDispatchCoordinator workerCoordinator;
    private final PollingFallback pollingFallback;
    private final NotificationPayloadParser payloadParser;
    private final Duration initialReconnectDelay;
    private final Duration maxReconnectDelay;
    private final Duration notificationWait;
    private final Duration shutdownTimeout;
    private final Sleeper sleeper;
    private final ExecutorService executor;
    private final ManagedRuntimeMetrics metrics;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<Connection> activeConnection = new AtomicReference<>();

    public PostgreSQLNotificationListener(DataSource dataSource,
                                          WorkerDispatchCoordinator workerCoordinator,
                                          PollingFallback pollingFallback,
                                          Duration initialReconnectDelay,
                                          Duration maxReconnectDelay,
                                          Duration shutdownTimeout) {
        this(dataSource::getConnection, workerCoordinator, pollingFallback,
                initialReconnectDelay, maxReconnectDelay, DEFAULT_NOTIFICATION_WAIT,
                shutdownTimeout, PostgreSQLNotificationListener::sleep,
                            Executors.newSingleThreadExecutor(new ListenerThreadFactory()),
                            ManagedRuntimeMetrics.noop());
                        }

                        public PostgreSQLNotificationListener(DataSource dataSource,
                                          WorkerDispatchCoordinator workerCoordinator,
                                          PollingFallback pollingFallback,
                                          Duration initialReconnectDelay,
                                          Duration maxReconnectDelay,
                                          Duration shutdownTimeout,
                                          ManagedRuntimeMetrics metrics) {
                        this(dataSource::getConnection, workerCoordinator, pollingFallback,
                            initialReconnectDelay, maxReconnectDelay, DEFAULT_NOTIFICATION_WAIT,
                            shutdownTimeout, PostgreSQLNotificationListener::sleep,
                            Executors.newSingleThreadExecutor(new ListenerThreadFactory()), metrics);
    }

    PostgreSQLNotificationListener(ConnectionProvider connectionProvider,
                                   WorkerDispatchCoordinator workerCoordinator,
                                   PollingFallback pollingFallback,
                                   Duration initialReconnectDelay,
                                   Duration maxReconnectDelay,
                                   Duration notificationWait,
                                   Duration shutdownTimeout,
                                   Sleeper sleeper,
                                   ExecutorService executor) {
                    this(connectionProvider, workerCoordinator, pollingFallback, initialReconnectDelay,
                        maxReconnectDelay, notificationWait, shutdownTimeout, sleeper, executor,
                        ManagedRuntimeMetrics.noop());
                    }

                    PostgreSQLNotificationListener(ConnectionProvider connectionProvider,
                                   WorkerDispatchCoordinator workerCoordinator,
                                   PollingFallback pollingFallback,
                                   Duration initialReconnectDelay,
                                   Duration maxReconnectDelay,
                                   Duration notificationWait,
                                   Duration shutdownTimeout,
                                   Sleeper sleeper,
                                   ExecutorService executor,
                                   ManagedRuntimeMetrics metrics) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider,
                "connectionProvider must not be null");
        this.workerCoordinator = Objects.requireNonNull(workerCoordinator,
                "workerCoordinator must not be null");
        this.pollingFallback = Objects.requireNonNull(pollingFallback, "pollingFallback must not be null");
        this.payloadParser = new NotificationPayloadParser();
        this.initialReconnectDelay = positive(initialReconnectDelay, "initialReconnectDelay");
        this.maxReconnectDelay = positive(maxReconnectDelay, "maxReconnectDelay");
        if (this.maxReconnectDelay.compareTo(this.initialReconnectDelay) < 0) {
            throw new IllegalArgumentException("maxReconnectDelay must not be less than initialReconnectDelay");
        }
        this.notificationWait = positive(notificationWait, "notificationWait");
        this.shutdownTimeout = positive(shutdownTimeout, "shutdownTimeout");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        metrics.updateListenerConnected(false);
        executor.execute(this::listenLoop);
    }

    public void stop() {
        running.set(false);
        metrics.updateListenerConnected(false);
        closeActiveConnection();
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warn("PostgreSQL notification listener did not stop within the configured timeout");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while stopping the PostgreSQL notification listener");
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public boolean isConnected() {
        return activeConnection.get() != null;
    }

    @Override
    public void close() {
        stop();
    }

    private void listenLoop() {
        Duration reconnectDelay = initialReconnectDelay;
        while (running.get()) {
            Connection connection = null;
            try {
                connection = connectionProvider.getConnection();
                activeConnection.set(connection);
                PGConnection pgConnection = connection.unwrap(PGConnection.class);
                subscribe(connection);
                metrics.updateListenerConnected(true);
                pollingFallback.onListenerReconnected();
                reconnectDelay = initialReconnectDelay;
                consume(pgConnection);
            } catch (SQLException | RuntimeException exception) {
                if (running.get()) {
                    LOG.warn("PostgreSQL notification listener reconnecting after {}",
                            exception.getClass().getSimpleName());
                    if (!waitBeforeReconnect(reconnectDelay)) {
                        break;
                    }
                    reconnectDelay = nextDelay(reconnectDelay, maxReconnectDelay);
                }
            } finally {
                if (connection != null) {
                    activeConnection.compareAndSet(connection, null);
                    metrics.updateListenerConnected(false);
                    closeQuietly(connection);
                }
            }
        }
    }

    private void subscribe(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("LISTEN " + RunNotificationPublisher.RUN_CHANNEL);
            statement.execute("LISTEN " + RunNotificationPublisher.CONTROL_CHANNEL);
        }
    }

    private void consume(PGConnection pgConnection) throws SQLException {
        int waitMillis = Math.toIntExact(notificationWait.toMillis());
        while (running.get()) {
            PGNotification[] notifications = pgConnection.getNotifications(waitMillis);
            if (notifications == null) {
                continue;
            }
            for (PGNotification notification : notifications) {
                route(notification);
            }
        }
    }

    private void route(PGNotification notification) {
        if (notification == null) {
            metrics.recordNotificationReceived(null, false);
            return;
        }
        String channel = notification.getName();
        if (!RunNotificationPublisher.RUN_CHANNEL.equals(channel)
                && !RunNotificationPublisher.CONTROL_CHANNEL.equals(channel)) {
            metrics.recordNotificationReceived(channel, false);
            LOG.debug("Ignoring notification on unsupported channel");
            return;
        }
        Optional<UUID> runId = payloadParser.parse(notification.getParameter());
        metrics.recordNotificationReceived(channel, runId.isPresent());
        if (runId.isEmpty()) {
            LOG.warn("Ignoring malformed notification on channel {}", channel);
            return;
        }
        if (RunNotificationPublisher.RUN_CHANNEL.equals(channel)) {
            workerCoordinator.signalRun(runId.orElseThrow(), System.nanoTime());
        } else {
            workerCoordinator.signalCancellation(runId.orElseThrow());
        }
    }

    private boolean waitBeforeReconnect(Duration delay) {
        try {
            sleeper.sleep(delay);
            return running.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            running.set(false);
            return false;
        }
    }

    private void closeActiveConnection() {
        Connection connection = activeConnection.getAndSet(null);
        if (connection != null) {
            closeQuietly(connection);
        }
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (SQLException exception) {
            LOG.debug("Could not close PostgreSQL notification connection");
        }
    }

    private static Duration nextDelay(Duration current, Duration maximum) {
        long currentMillis = current.toMillis();
        long maximumMillis = maximum.toMillis();
        long doubled = currentMillis > maximumMillis / 2 ? maximumMillis : currentMillis * 2;
        return Duration.ofMillis(Math.min(doubled, maximumMillis));
    }

    private static Duration positive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        if (value.toMillis() < 1) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
        return value;
    }

    private static void sleep(Duration duration) throws InterruptedException {
        Thread.sleep(duration.toMillis());
    }

    @FunctionalInterface
    interface ConnectionProvider {
        Connection getConnection() throws SQLException;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private static final class ListenerThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "ReplicadbPostgreSQLListener-" + sequence.incrementAndGet());
        }
    }
}
