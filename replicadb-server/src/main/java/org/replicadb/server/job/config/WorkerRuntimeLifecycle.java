package org.replicadb.server.job.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.HeartbeatService;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerRuntimeLifecycle implements SmartLifecycle {

    private static final Logger LOG = LogManager.getLogger(WorkerRuntimeLifecycle.class);

    private final WorkerDispatchCoordinator workerCoordinator;
    private final PollingFallback pollingFallback;
    private final PostgreSQLNotificationListener notificationListener;
    private final HeartbeatService heartbeatService;
    private final Duration shutdownTimeout;
    private final AtomicBoolean running = new AtomicBoolean();

    public WorkerRuntimeLifecycle(WorkerDispatchCoordinator workerCoordinator,
                                  PollingFallback pollingFallback,
                                  PostgreSQLNotificationListener notificationListener,
                                  HeartbeatService heartbeatService,
                                  Duration shutdownTimeout) {
        this.workerCoordinator = Objects.requireNonNull(workerCoordinator,
                "workerCoordinator must not be null");
        this.pollingFallback = Objects.requireNonNull(pollingFallback, "pollingFallback must not be null");
        this.notificationListener = Objects.requireNonNull(notificationListener,
                "notificationListener must not be null");
        this.heartbeatService = Objects.requireNonNull(heartbeatService, "heartbeatService must not be null");
        this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout must not be null");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        workerCoordinator.startAccepting();
        pollingFallback.start();
        try {
            notificationListener.start();
        } catch (RuntimeException exception) {
            LOG.warn("Worker notification listener failed to start with {}",
                    exception.getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        workerCoordinator.stopAccepting();
        pollingFallback.stop();
        notificationListener.stop();
        heartbeatService.shutdown();
        workerCoordinator.shutdown(shutdownTimeout);
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}