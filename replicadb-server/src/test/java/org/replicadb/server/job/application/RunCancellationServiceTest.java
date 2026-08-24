package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;

import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunCancellationServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
        private final RunNotificationPublisher notificationPublisher = mock(RunNotificationPublisher.class);
        private final RunCancellationService service = new RunCancellationService(runStore, notificationPublisher);

    @Test
    void persistsCancellationBeforeSignallingLocalExecution() {
        UUID runId = UUID.randomUUID();
        Consumer<UUID> localSignal = mock(Consumer.class);
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.REQUESTED);

        assertEquals(JobRunStore.CancellationResult.REQUESTED,
                service.requestCancellation(runId, "warning", localSignal));

        var order = inOrder(runStore, notificationPublisher, localSignal);
        order.verify(runStore).requestCancellation(runId, "warning");
        order.verify(notificationPublisher).publishCancellation(runId);
        order.verify(localSignal).accept(runId);
    }

    @Test
    void persistsCancellationEvenWithoutLocalExecutionRegistration() {
        UUID runId = UUID.randomUUID();
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.REQUESTED);

        assertEquals(JobRunStore.CancellationResult.REQUESTED,
                service.requestCancellation(runId, "warning", ignored -> { }));

        verify(runStore).requestCancellation(runId, "warning");
        verify(notificationPublisher).publishCancellation(runId);
    }

    @Test
    void doesNotSignalTerminalRun() {
        UUID runId = UUID.randomUUID();
        Consumer<UUID> localSignal = mock(Consumer.class);
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.TERMINAL);

        assertEquals(JobRunStore.CancellationResult.TERMINAL,
                service.requestCancellation(runId, "warning", localSignal));

        verify(notificationPublisher, never()).publishCancellation(runId);
        verify(localSignal, never()).accept(runId);
    }

    @Test
    void notificationFailureDoesNotRejectDurableCancellation() {
        UUID runId = UUID.randomUUID();
        Consumer<UUID> localSignal = mock(Consumer.class);
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.REQUESTED);
        doThrow(new IllegalStateException("notification unavailable"))
                .when(notificationPublisher).publishCancellation(runId);

        assertEquals(JobRunStore.CancellationResult.REQUESTED,
                service.requestCancellation(runId, "warning", localSignal));

        verify(localSignal).accept(runId);
    }

    @Test
    void repeatedCancellationStillPublishesAControlWakeup() {
        UUID runId = UUID.randomUUID();
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.ALREADY_REQUESTED);

        assertEquals(JobRunStore.CancellationResult.ALREADY_REQUESTED,
                service.requestCancellation(runId, "warning", ignored -> { }));

        verify(notificationPublisher).publishCancellation(runId);
    }
}
