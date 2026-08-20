package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.port.JobRunStore;

import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunCancellationServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
    private final RunCancellationService service = new RunCancellationService(runStore);

    @Test
    void persistsCancellationBeforeSignallingLocalExecution() {
        UUID runId = UUID.randomUUID();
        Consumer<UUID> localSignal = mock(Consumer.class);
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.REQUESTED);

        assertEquals(JobRunStore.CancellationResult.REQUESTED,
                service.requestCancellation(runId, "warning", localSignal));

        var order = inOrder(runStore, localSignal);
        order.verify(runStore).requestCancellation(runId, "warning");
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
    }

    @Test
    void doesNotSignalTerminalRun() {
        UUID runId = UUID.randomUUID();
        Consumer<UUID> localSignal = mock(Consumer.class);
        when(runStore.requestCancellation(runId, "warning"))
                .thenReturn(JobRunStore.CancellationResult.TERMINAL);

        assertEquals(JobRunStore.CancellationResult.TERMINAL,
                service.requestCancellation(runId, "warning", localSignal));

        verify(localSignal, never()).accept(runId);
    }
}
