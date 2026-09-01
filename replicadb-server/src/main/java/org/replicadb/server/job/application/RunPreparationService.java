package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public final class RunPreparationService {

    private final RunLeaseService runLeaseService;

    public RunPreparationService(RunLeaseService runLeaseService) {
        this.runLeaseService = Objects.requireNonNull(runLeaseService, "runLeaseService must not be null");
    }

    public Optional<ClaimedRunPreparation> claimNextEligible(String executorIdentity,
                                                             Duration leaseDuration) {
        return runLeaseService.claimAndPrepare(null, executorIdentity, leaseDuration);
    }

    public Optional<ClaimedRunPreparation> claimRequested(UUID runId, String executorIdentity,
                                                           Duration leaseDuration) {
        Objects.requireNonNull(runId, "runId must not be null");
        return runLeaseService.claimAndPrepare(runId, executorIdentity, leaseDuration);
    }
}
