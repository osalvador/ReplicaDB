package org.replicadb.server.job.execution;

import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public final class ActiveRunRegistry {

    private final ConcurrentMap<UUID, RunExecutionHandle> activeRuns = new ConcurrentHashMap<>();

    public boolean register(RunExecutionHandle handle) {
        Objects.requireNonNull(handle, "handle must not be null");
        return activeRuns.putIfAbsent(handle.runId(), handle) == null;
    }

    public boolean remove(UUID runId, RunExecutionHandle handle) {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(handle, "handle must not be null");
        return activeRuns.remove(runId, handle);
    }

    public Optional<RunExecutionHandle> remove(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        return Optional.ofNullable(activeRuns.remove(runId));
    }

    public Optional<RunExecutionHandle> find(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        return Optional.ofNullable(activeRuns.get(runId));
    }

    public boolean requestCancellation(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        RunExecutionHandle handle = activeRuns.get(runId);
        if (handle == null) {
            return false;
        }
        handle.requestCancellation();
        return true;
    }

    public int requestCancellationForAll() {
        int requested = 0;
        for (RunExecutionHandle handle : activeRuns.values()) {
            handle.requestCancellation();
            requested++;
        }
        return requested;
    }
}