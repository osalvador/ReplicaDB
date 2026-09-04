package org.replicadb.server.local;

import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.util.Objects;

public final class EmbeddedPostgresShutdownLifecycle implements SmartLifecycle {

    private final EmbeddedPostgresRuntime runtime;
    private boolean running = true;

    public EmbeddedPostgresShutdownLifecycle(EmbeddedPostgresRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime must not be null");
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        try {
            runtime.close();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not close embedded PostgreSQL runtime", exception);
        } finally {
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running && !runtime.isClosed();
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }
}
