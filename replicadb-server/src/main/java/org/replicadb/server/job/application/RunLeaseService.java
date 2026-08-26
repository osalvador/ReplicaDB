package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.job.execution.AdmissionLane;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public final class RunLeaseService {

    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(5);

    private final JobRunStore runStore;
    private final ManagedRuntimeMetrics metrics;

    @Autowired
    public RunLeaseService(JobRunStore runStore, ManagedRuntimeMetrics metrics) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    @Deprecated
    public RunLeaseService(JobRunStore runStore) {
        this(runStore, ManagedRuntimeMetrics.noop());
    }

    public Optional<JobRun> claimNextEligible(String executorIdentity, Duration leaseDuration) {
        validateExecutorIdentity(executorIdentity);
        validateLeaseDuration(leaseDuration);
        return claim(null, executorIdentity, leaseDuration, "queue");
    }

    public Optional<JobRun> claimRequested(UUID runId, String executorIdentity, Duration leaseDuration) {
        Objects.requireNonNull(runId, "runId must not be null");
        validateExecutorIdentity(executorIdentity);
        validateLeaseDuration(leaseDuration);
        return claim(runId, executorIdentity, leaseDuration, "directed");
    }

    public Optional<JobRun> claimFallback(String executorIdentity, Duration leaseDuration) {
        validateExecutorIdentity(executorIdentity);
        validateLeaseDuration(leaseDuration);
        return claim(null, executorIdentity, leaseDuration, "fallback");
    }

    public Optional<JobRun> claimNextEligible(String executorIdentity) {
        return claimNextEligible(executorIdentity, DEFAULT_LEASE_DURATION);
    }

    public JobRunStore.LeaseRenewalResult renewLease(UUID runId, LeaseToken leaseToken,
                                                     Duration leaseDuration) {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(leaseToken, "leaseToken must not be null");
        validateLeaseDuration(leaseDuration);
        try {
            JobRunStore.LeaseRenewalResult result = runStore.renewLease(runId, leaseToken, leaseDuration);
            metrics.recordLeaseRenewal(result);
            return result;
        } catch (RuntimeException exception) {
            metrics.recordLeaseRenewal("error");
            throw exception;
        }
    }

    private Optional<JobRun> claim(UUID requestedRunId, String executorIdentity, Duration leaseDuration,
                                   String claimType) {
        try {
            Optional<JobRun> result = runStore.claimNextEligible(requestedRunId, executorIdentity, leaseDuration);
            metrics.recordClaim(claimType, result.isPresent() ? "claimed" : "empty");
            return result;
        } catch (RuntimeException exception) {
            metrics.recordClaim(claimType, "error");
            throw exception;
        }
    }

    private static void validateExecutorIdentity(String executorIdentity) {
        if (executorIdentity == null || executorIdentity.isBlank()) {
            throw new IllegalArgumentException("executorIdentity must not be blank");
        }
    }

    private static void validateLeaseDuration(Duration leaseDuration) {
        if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
    }
}
