package org.replicadb.server.job.application;

import org.replicadb.server.job.port.JobRunStore;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public final class RunCancellationService {

    private final JobRunStore runStore;

    public RunCancellationService(JobRunStore runStore) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
    }

    public JobRunStore.CancellationResult requestCancellation(UUID runId, String cancellationWarning,
                                                               Consumer<UUID> localSignal) {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(localSignal, "localSignal must not be null");
        JobRunStore.CancellationResult result = runStore.requestCancellation(runId, cancellationWarning);
        if (result == JobRunStore.CancellationResult.REQUESTED
                || result == JobRunStore.CancellationResult.ALREADY_REQUESTED) {
            localSignal.accept(runId);
        }
        return result;
    }

    public JobRunStore.CancellationResult cancelPending(UUID runId, String cancellationWarning) {
        Objects.requireNonNull(runId, "runId must not be null");
        return runStore.cancelPending(runId, cancellationWarning);
    }
}
