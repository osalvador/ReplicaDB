package org.replicadb.server.job.application;

import org.replicadb.server.job.port.JobRunStore;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Service
public final class RunRecoveryService {

    private final JobRunStore runStore;

    public RunRecoveryService(JobRunStore runStore) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
    }

    public RunRecoveryResult recoverExpiredRun(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        return runStore.recoverExpiredRun(runId);
    }
}
