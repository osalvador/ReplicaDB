package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public final class RunLeaseService {

    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(5);

    private final JobRunStore runStore;

    public RunLeaseService(JobRunStore runStore) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
    }

    public Optional<JobRun> claimNextEligible(String executorIdentity, Duration leaseDuration) {
        validateExecutorIdentity(executorIdentity);
        validateLeaseDuration(leaseDuration);
        return runStore.claimNextEligible(null, executorIdentity, leaseDuration);
    }

    public Optional<JobRun> claimRequested(UUID runId, String executorIdentity, Duration leaseDuration) {
        Objects.requireNonNull(runId, "runId must not be null");
        validateExecutorIdentity(executorIdentity);
        validateLeaseDuration(leaseDuration);
        return runStore.claimNextEligible(runId, executorIdentity, leaseDuration);
    }

    public Optional<JobRun> claimNextEligible(String executorIdentity) {
        return claimNextEligible(executorIdentity, DEFAULT_LEASE_DURATION);
    }

    public JobRunStore.LeaseRenewalResult renewLease(UUID runId, LeaseToken leaseToken,
                                                     Duration leaseDuration) {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(leaseToken, "leaseToken must not be null");
        validateLeaseDuration(leaseDuration);
        return runStore.renewLease(runId, leaseToken, leaseDuration);
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
