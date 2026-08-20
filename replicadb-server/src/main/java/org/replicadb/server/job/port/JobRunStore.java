package org.replicadb.server.job.port;

import org.replicadb.server.job.application.RunRecoveryResult;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface JobRunStore {

    JobRun insertPending(UUID jobDefinitionId, UUID previousRunId, int attempt, Instant availableAt);

    Optional<JobRun> findById(UUID id);

    boolean hasActiveRun(UUID jobDefinitionId);

    Optional<JobRun> claimNextEligible(UUID requestedRunId, String executorIdentity, Duration leaseDuration);

    LeaseRenewalResult renewLease(UUID runId, LeaseToken leaseToken, Duration leaseDuration);

    RunRecoveryResult recoverExpiredRun(UUID runId);

    CancellationResult requestCancellation(UUID runId, String cancellationWarning);

    CancellationResult cancelPending(UUID runId, String cancellationWarning);

    JobRun scheduleRetry(UUID failedRunId, Instant availableAt);

    Optional<String> findLastCommittedWatermark(UUID jobDefinitionId);

    FencedUpdateResult recordProgress(UUID runId, LeaseToken leaseToken,
                                      long rowsProcessed, long durationMillis);

    FencedUpdateResult markSucceeded(UUID runId, LeaseToken leaseToken,
                                     long rowsProcessed, long durationMillis,
                                     String committedWatermark);

    FencedUpdateResult markFailed(UUID runId, LeaseToken leaseToken,
                                  long rowsProcessed, long durationMillis,
                                  String errorMessage);

    FencedUpdateResult markCancelled(UUID runId, LeaseToken leaseToken,
                                     long rowsProcessed, long durationMillis);

    List<JobRun> findPage(UUID jobDefinitionId, JobRunStatus status, int page, int size,
                          Set<UUID> restrictToJobIds);

    long count(UUID jobDefinitionId, JobRunStatus status, Set<UUID> restrictToJobIds);

    enum LeaseRenewalResult {
        RENEWED,
        FENCED,
        NOT_FOUND
    }

    enum FencedUpdateResult {
        UPDATED,
        FENCED,
        NOT_FOUND
    }

    enum CancellationResult {
        REQUESTED,
        ALREADY_REQUESTED,
        CANCELLED,
        TERMINAL,
        NOT_FOUND
    }
}
