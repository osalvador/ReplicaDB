package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.persistence.RunTriggerIdempotencyRepository;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunDispatchServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
    private final RunTriggerIdempotencyRepository idempotencyRepository =
            mock(RunTriggerIdempotencyRepository.class);
    private final RunNotificationPublisher notificationPublisher = mock(RunNotificationPublisher.class);
    private final RunDispatchService service = new RunDispatchService(
            runStore, idempotencyRepository, notificationPublisher);

    @Test
    void createsManualRunAndPublishesAfterPersistence() {
        UUID jobDefinitionId = UUID.randomUUID();
        UUID runId = UUID.randomUUID();
        JobRun run = run(runId);
        when(idempotencyRepository.reserve(eq("manual-key"), eq(jobDefinitionId), any(UUID.class)))
                .thenReturn(Optional.of(new RunTriggerIdempotencyRepository.IdempotencyEntry(
                        jobDefinitionId, runId)));
        when(runStore.insertPendingNow(any(UUID.class), eq(jobDefinitionId), isNull(), eq(1))).thenReturn(run);

        RunDispatchResult result = service.dispatchManual(jobDefinitionId, "manual-key");

        assertTrue(result.created());
        assertEquals(RunDispatchResult.Outcome.CREATED, result.outcome());
        assertEquals(run, result.run().orElseThrow());
        var order = inOrder(idempotencyRepository, runStore, notificationPublisher);
        order.verify(idempotencyRepository).reserve(eq("manual-key"), eq(jobDefinitionId), any(UUID.class));
        order.verify(runStore).insertPendingNow(any(UUID.class), eq(jobDefinitionId), isNull(), eq(1));
        order.verify(notificationPublisher).publishRun(run.id());
    }

    @Test
    void replaysARecentSameJobKeyWithoutCreatingOrPublishing() {
        UUID jobDefinitionId = UUID.randomUUID();
        UUID runId = UUID.randomUUID();
        JobRun run = run(runId);
        when(idempotencyRepository.reserve(eq("replay-key"), eq(jobDefinitionId), any(UUID.class)))
                .thenReturn(Optional.empty());
        when(idempotencyRepository.findValidEntry("replay-key")).thenReturn(Optional.of(
                new RunTriggerIdempotencyRepository.IdempotencyEntry(jobDefinitionId, runId)));
        when(runStore.findById(runId)).thenReturn(Optional.of(run));

        RunDispatchResult result = service.dispatchManual(jobDefinitionId, "replay-key");

        assertTrue(result.replayed());
        assertEquals(run, result.run().orElseThrow());
        verify(runStore, never()).insertPendingNow(any(UUID.class), any(UUID.class), isNull(), eq(1));
        verify(notificationPublisher, never()).publishRun(any(UUID.class));
    }

    @Test
    void rejectsARecentKeyForAnotherJob() {
        UUID requestedJob = UUID.randomUUID();
        UUID existingJob = UUID.randomUUID();
        when(idempotencyRepository.reserve(eq("conflict-key"), eq(requestedJob), any(UUID.class)))
                .thenReturn(Optional.empty());
        when(idempotencyRepository.findValidEntry("conflict-key")).thenReturn(Optional.of(
                new RunTriggerIdempotencyRepository.IdempotencyEntry(existingJob, UUID.randomUUID())));

        assertThrows(IllegalStateException.class,
                () -> service.dispatchManual(requestedJob, "conflict-key"));
        verify(runStore, never()).insertPendingNow(any(UUID.class), any(UUID.class), isNull(), eq(1));
        verify(notificationPublisher, never()).publishRun(any(UUID.class));
    }

    @Test
    void seedsAndCancelsWithoutPublishingExecution() {
        UUID jobDefinitionId = UUID.randomUUID();
        UUID runId = UUID.randomUUID();
        JobRun pending = run(runId);
        JobRun cancelled = run(runId);
        when(idempotencyRepository.reserve(eq("seed-key"), eq(jobDefinitionId), any(UUID.class)))
                .thenReturn(Optional.of(new RunTriggerIdempotencyRepository.IdempotencyEntry(
                        jobDefinitionId, runId)));
        when(runStore.insertPendingNow(any(UUID.class), eq(jobDefinitionId), isNull(), eq(1))).thenReturn(pending);
        when(runStore.cancelPending(runId, "seed warning"))
                .thenReturn(JobRunStore.CancellationResult.CANCELLED);
        when(runStore.findById(runId)).thenReturn(Optional.of(cancelled));

        RunDispatchResult result = service.dispatchManual(jobDefinitionId, "seed-key", true, "seed warning");

        assertTrue(result.created());
        assertEquals(cancelled, result.run().orElseThrow());
        verify(notificationPublisher, never()).publishRun(any(UUID.class));
    }

    @Test
    void PublishesScheduledRetryAndRecoveryReplacementOnlyWhenCreated() {
        UUID jobDefinitionId = UUID.randomUUID();
        JobRun scheduled = run(UUID.randomUUID());
        JobRun retry = run(UUID.randomUUID());
        JobRun replacement = run(UUID.randomUUID());
        UUID failedRunId = UUID.randomUUID();
        UUID expiredRunId = UUID.randomUUID();
        when(runStore.insertPendingNow(jobDefinitionId, null, 1)).thenReturn(scheduled);
        when(runStore.scheduleRetryNow(failedRunId)).thenReturn(retry);
        JobRun abandoned = run(expiredRunId);
        when(runStore.recoverExpiredRun(expiredRunId)).thenReturn(new RunRecoveryResult(
                Optional.of(abandoned), Optional.of(replacement)));

        assertTrue(service.dispatchScheduled(jobDefinitionId).created());
        assertTrue(service.dispatchRetry(failedRunId).created());
        assertTrue(service.recoverExpiredRun(expiredRunId).replacementCreated());
        verify(notificationPublisher).publishRun(scheduled.id());
        verify(notificationPublisher).publishRun(retry.id());
        verify(notificationPublisher).publishRun(replacement.id());
    }

    @Test
    void DoesNotPublishWhenRecoveryHasNoReplacement() {
        UUID expiredRunId = UUID.randomUUID();
        JobRun abandoned = run(expiredRunId);
        when(runStore.recoverExpiredRun(expiredRunId)).thenReturn(new RunRecoveryResult(
                Optional.of(abandoned), Optional.empty()));

        RunDispatchResult result = service.recoverExpiredRun(expiredRunId);

        assertEquals(RunDispatchResult.Outcome.RECOVERY_NOOP, result.outcome());
        verify(notificationPublisher, never()).publishRun(any(UUID.class));
    }

    @Test
    void rejectsManualAndScheduledDispatchWhenBindingsAreDisabled() {
        UUID jobDefinitionId = UUID.randomUUID();
        UUID reservedRunId = UUID.randomUUID();
        when(idempotencyRepository.reserve(eq("disabled-key"), eq(jobDefinitionId), any(UUID.class)))
                .thenReturn(Optional.of(new RunTriggerIdempotencyRepository.IdempotencyEntry(
                        jobDefinitionId, reservedRunId)));
        doThrow(new IllegalStateException("disabled binding"))
                .when(runStore).requireBindingsEnabled(jobDefinitionId);

        assertThrows(IllegalStateException.class,
                () -> service.dispatchManual(jobDefinitionId, "disabled-key"));
        assertThrows(IllegalStateException.class,
                () -> service.dispatchScheduled(jobDefinitionId));

        verify(runStore, never()).insertPendingNow(any(UUID.class), any(UUID.class), isNull(), eq(1));
        verify(runStore, never()).insertPendingNow(jobDefinitionId, null, 1);
        verify(notificationPublisher, never()).publishRun(any(UUID.class));
    }

    private static JobRun run(UUID runId) {
        JobRun run = mock(JobRun.class);
        when(run.id()).thenReturn(runId);
        return run;
    }
}
