package org.replicadb.server.job.execution;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class HeartbeatHandle implements AutoCloseable {

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Runnable stopAction;

    HeartbeatHandle(Runnable stopAction) {
        this.stopAction = Objects.requireNonNull(stopAction, "stopAction must not be null");
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            stopAction.run();
        }
    }

    public boolean isStopped() {
        return stopped.get();
    }

    @Override
    public void close() {
        stop();
    }
}