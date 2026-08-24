package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.persistence.RunTriggerIdempotencyRepository;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class RunDispatchService {

    private final JobRunStore runStore;
    private final RunTriggerIdempotencyRepository idempotencyRepository;
    private final RunNotificationPublisher notificationPublisher;

    public RunDispatchService(JobRunStore runStore,
                              RunTriggerIdempotencyRepository idempotencyRepository,
                              RunNotificationPublisher notificationPublisher) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
        this.idempotencyRepository = Objects.requireNonNull(idempotencyRepository,
                "idempotencyRepository must not be null");
        this.notificationPublisher = Objects.requireNonNull(notificationPublisher,
                "notificationPublisher must not be null");
    }

    @Transactional
    public RunDispatchResult dispatchManual(UUID jobDefinitionId, String idempotencyKey) {
        return dispatchManualInternal(jobDefinitionId, idempotencyKey, false, null);
    }

    @Transactional
    public RunDispatchResult dispatchManual(UUID jobDefinitionId, String idempotencyKey,
                                            boolean localSeedRequested, String cancellationWarning) {
        return dispatchManualInternal(jobDefinitionId, idempotencyKey, localSeedRequested, cancellationWarning);
    }

    private RunDispatchResult dispatchManualInternal(UUID jobDefinitionId, String idempotencyKey,
                                                     boolean localSeedRequested, String cancellationWarning) {
        Objects.requireNonNull(jobDefinitionId, "jobDefinitionId must not be null");
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (localSeedRequested && cancellationWarning == null) {
            throw new IllegalArgumentException("cancellationWarning must not be null for local seed");
        }

        UUID runId = UUID.randomUUID();
        Optional<RunTriggerIdempotencyRepository.IdempotencyEntry> reserved =
                idempotencyRepository.reserve(idempotencyKey, jobDefinitionId, runId);
        if (reserved.isEmpty()) {
            RunTriggerIdempotencyRepository.IdempotencyEntry existing = idempotencyRepository
                    .findValidEntry(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency-Key reservation disappeared: " + idempotencyKey));
            if (!jobDefinitionId.equals(existing.jobDefinitionId())) {
                throw new IllegalStateException("Idempotency-Key is already used for another job");
            }
            JobRun replay = runStore.findById(existing.runId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency-Key refers to missing JobRun " + existing.runId()));
            return new RunDispatchResult(Optional.of(replay), RunDispatchResult.Outcome.REPLAYED);
        }

        JobRun pending = runStore.insertPendingNow(runId, jobDefinitionId, null, 1);
        if (localSeedRequested) {
            JobRunStore.CancellationResult cancellation = runStore.cancelPending(
                    pending.id(), cancellationWarning);
            if (cancellation != JobRunStore.CancellationResult.CANCELLED) {
                throw new IllegalStateException("Could not cancel local seed JobRun " + pending.id());
            }
            JobRun cancelled = runStore.findById(pending.id()).orElse(pending);
            return new RunDispatchResult(Optional.of(cancelled), RunDispatchResult.Outcome.CREATED);
        }

        notificationPublisher.publishRun(pending.id());
        return new RunDispatchResult(Optional.of(pending), RunDispatchResult.Outcome.CREATED);
    }

    @Transactional
    public RunDispatchResult dispatchScheduled(UUID jobDefinitionId) {
        Objects.requireNonNull(jobDefinitionId, "jobDefinitionId must not be null");
        JobRun pending = runStore.insertPendingNow(jobDefinitionId, null, 1);
        notificationPublisher.publishRun(pending.id());
        return new RunDispatchResult(Optional.of(pending), RunDispatchResult.Outcome.CREATED);
    }

    @Transactional
    public RunDispatchResult dispatchRetry(UUID failedRunId) {
        Objects.requireNonNull(failedRunId, "failedRunId must not be null");
        JobRun retry = runStore.scheduleRetryNow(failedRunId);
        notificationPublisher.publishRun(retry.id());
        return new RunDispatchResult(Optional.of(retry), RunDispatchResult.Outcome.CREATED);
    }

    @Transactional
    public RunDispatchResult recoverExpiredRun(UUID runId) {
        Objects.requireNonNull(runId, "runId must not be null");
        RunRecoveryResult recovery = runStore.recoverExpiredRun(runId);
        if (recovery.replacementRun().isPresent()) {
            JobRun replacement = recovery.replacementRun().orElseThrow();
            notificationPublisher.publishRun(replacement.id());
            return new RunDispatchResult(Optional.of(replacement),
                    RunDispatchResult.Outcome.RECOVERY_REPLACEMENT);
        }
        return new RunDispatchResult(recovery.abandonedRun(), RunDispatchResult.Outcome.RECOVERY_NOOP);
    }
}