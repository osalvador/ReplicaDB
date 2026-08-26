package org.replicadb.server.job.execution;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class WorkerAdmissionScheduler implements AutoCloseable {

    private final ScheduledExecutorService executor;

    public WorkerAdmissionScheduler() {
        this(Executors.newSingleThreadScheduledExecutor(new AdmissionThreadFactory()));
    }

    public WorkerAdmissionScheduler(ScheduledExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    public ScheduledFuture<?> schedule(Runnable action, Duration delay) {
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(delay, "delay must not be null");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        return executor.schedule(action, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public void shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    @Override
    public void close() {
        shutdown(Duration.ofSeconds(30));
    }

    private static final class AdmissionThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "ReplicadbWorkerAdmission-" + sequence.incrementAndGet());
        }
    }
}