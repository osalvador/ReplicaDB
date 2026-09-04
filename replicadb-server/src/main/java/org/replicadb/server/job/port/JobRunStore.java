package org.replicadb.server.job.port;

import org.replicadb.server.job.application.RunRecoveryResult;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.domain.ClaimedRunPreparation;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface JobRunStore {

    JobRun insertPendingNow(UUID jobDefinitionId, UUID previousRunId, int attempt);

    JobRun insertPendingNow(UUID runId, UUID jobDefinitionId, UUID previousRunId, int attempt);

    Optional<JobRun> findById(UUID id);

    boolean hasActiveRun(UUID jobDefinitionId);

    void requireBindingsEnabled(UUID jobDefinitionId);

    Optional<JobRun> claimNextEligible(UUID requestedRunId, String executorIdentity, Duration leaseDuration);

    Optional<ClaimedRunPreparation> claimAndPrepare(UUID requestedRunId, String executorIdentity,
                                                     Duration leaseDuration);

    LeaseRenewalResult renewLease(UUID runId, LeaseToken leaseToken, Duration leaseDuration);

    RunRecoveryResult recoverExpiredRun(UUID runId);

    List<UUID> findExpiredRunIds(int limit);

    List<UUID> findCancellationRequestedRunIds(String executorIdentity, int limit);

    EligibleRunSnapshot findEligibleRunSnapshot(int limit);

    CancellationResult requestCancellation(UUID runId, String cancellationWarning);

    CancellationResult cancelPending(UUID runId, String cancellationWarning);

    JobRun scheduleRetryNow(UUID failedRunId);

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

    List<JobRun> findPage(UUID jobDefinitionId, Set<JobRunStatus> statuses, Instant from, Instant to,
                          int page, int size, Set<UUID> restrictToJobIds);

    long count(UUID jobDefinitionId, Set<JobRunStatus> statuses, Instant from, Instant to,
               Set<UUID> restrictToJobIds);

        DashboardRunSummary summarizeDashboard(Instant from, Instant to, Set<UUID> restrictToJobIds);

        record DashboardRunSummary(
            long activeRuns,
            long totalRuns,
            long succeededRuns,
            long failedRuns,
            long rowsProcessed,
            long averageDurationMillis,
            long averageLatencyMillis,
            List<OutcomeBucket> outcomeBuckets,
            List<JobPerformance> jobPerformance) {
        }

        record OutcomeBucket(Instant bucket, long succeeded, long failed, long active) {
        }

        record JobPerformance(UUID jobId, String jobName, long runCount, long rowsProcessed,
                  long averageDurationMillis, long averageLatencyMillis) {
        }

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

    record EligibleRunSnapshot(int eligibleCount, boolean truncated, Instant oldestAvailableAt) {
    }
}
