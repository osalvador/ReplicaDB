package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Service
public final class RunFinalizationService {

    private final JobRunStore runStore;
    private final ManagedRuntimeMetrics metrics;

    @Autowired
    public RunFinalizationService(JobRunStore runStore, ManagedRuntimeMetrics metrics) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    @Deprecated
    public RunFinalizationService(JobRunStore runStore) {
        this(runStore, ManagedRuntimeMetrics.noop());
    }

    public JobRunStore.FencedUpdateResult recordProgress(UUID runId, LeaseToken leaseToken,
                                                          long rowsProcessed, long durationMillis) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        try {
            JobRunStore.FencedUpdateResult result = runStore.recordProgress(
                    runId, leaseToken, rowsProcessed, durationMillis);
            metrics.recordFencedUpdate("progress", result);
            return result;
        } catch (RuntimeException exception) {
            metrics.recordFencedUpdate("progress", "error");
            throw exception;
        }
    }

    public JobRunStore.FencedUpdateResult markSucceeded(UUID runId, LeaseToken leaseToken,
                                                        long rowsProcessed, long durationMillis,
                                                        String committedWatermark) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        try {
            JobRunStore.FencedUpdateResult result = runStore.markSucceeded(
                    runId, leaseToken, rowsProcessed, durationMillis, committedWatermark);
            metrics.recordFencedUpdate("succeeded", result);
            if (result == JobRunStore.FencedUpdateResult.UPDATED) {
                metrics.recordTerminalOutcome(org.replicadb.server.job.domain.JobRunStatus.SUCCEEDED);
            }
            return result;
        } catch (RuntimeException exception) {
            metrics.recordFencedUpdate("succeeded", "error");
            throw exception;
        }
    }

    public JobRunStore.FencedUpdateResult markFailed(UUID runId, LeaseToken leaseToken,
                                                     long rowsProcessed, long durationMillis,
                                                     String errorMessage) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        try {
            JobRunStore.FencedUpdateResult result = runStore.markFailed(
                    runId, leaseToken, rowsProcessed, durationMillis, errorMessage);
            metrics.recordFencedUpdate("failed", result);
            if (result == JobRunStore.FencedUpdateResult.UPDATED) {
                metrics.recordTerminalOutcome(org.replicadb.server.job.domain.JobRunStatus.FAILED);
            }
            return result;
        } catch (RuntimeException exception) {
            metrics.recordFencedUpdate("failed", "error");
            throw exception;
        }
    }

    public JobRunStore.FencedUpdateResult markCancelled(UUID runId, LeaseToken leaseToken,
                                                        long rowsProcessed, long durationMillis) {
        validateExecutionValues(runId, leaseToken, rowsProcessed, durationMillis);
        try {
            JobRunStore.FencedUpdateResult result = runStore.markCancelled(
                    runId, leaseToken, rowsProcessed, durationMillis);
            metrics.recordFencedUpdate("cancelled", result);
            if (result == JobRunStore.FencedUpdateResult.UPDATED) {
                metrics.recordTerminalOutcome(org.replicadb.server.job.domain.JobRunStatus.CANCELLED);
                metrics.recordCancellation("completion", "updated");
            } else {
                metrics.recordCancellation("completion", result.name());
            }
            return result;
        } catch (RuntimeException exception) {
            metrics.recordFencedUpdate("cancelled", "error");
            metrics.recordCancellation("completion", "error");
            throw exception;
        }
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
