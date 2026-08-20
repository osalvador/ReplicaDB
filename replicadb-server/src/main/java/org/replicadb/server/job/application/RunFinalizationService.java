package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Service
public final class RunFinalizationService {

    private final JobRunStore runStore;

    public RunFinalizationService(JobRunStore runStore) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
    }

    public JobRunStore.FencedUpdateResult recordProgress(UUID runId, LeaseToken leaseToken,
                                                          long rowsProcessed, long durationMillis) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        return runStore.recordProgress(runId, leaseToken, rowsProcessed, durationMillis);
    }

    public JobRunStore.FencedUpdateResult markSucceeded(UUID runId, LeaseToken leaseToken,
                                                        long rowsProcessed, long durationMillis,
                                                        String committedWatermark) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        return runStore.markSucceeded(runId, leaseToken, rowsProcessed, durationMillis, committedWatermark);
    }

    public JobRunStore.FencedUpdateResult markFailed(UUID runId, LeaseToken leaseToken,
                                                     long rowsProcessed, long durationMillis,
                                                     String errorMessage) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        return runStore.markFailed(runId, leaseToken, rowsProcessed, durationMillis, errorMessage);
    }

    public JobRunStore.FencedUpdateResult markCancelled(UUID runId, LeaseToken leaseToken,
                                                        long rowsProcessed, long durationMillis) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        return runStore.markCancelled(runId, leaseToken, rowsProcessed, durationMillis);
    }

    private static void validateExecutionValues(UUID runId, LeaseToken leaseToken,
                                                long rowsProcessed, long durationMillis) {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(leaseToken, "leaseToken must not be null");
        if (rowsProcessed < 0) {
            throw new IllegalArgumentException("rowsProcessed must not be negative");
        }
        if (durationMillis < 0) {
            throw new IllegalArgumentException("durationMillis must not be negative");
        }
    }
}
