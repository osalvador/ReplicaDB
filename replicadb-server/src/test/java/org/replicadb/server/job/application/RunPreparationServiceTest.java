package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunPreparationServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
    private final RunLeaseService runLeaseService = new RunLeaseService(runStore);
    private final RunPreparationService service = new RunPreparationService(runLeaseService);

    @Test
    void delegatesQueuePreparation() {
        Duration leaseDuration = Duration.ofMinutes(5);
        when(runStore.claimAndPrepare(isNull(), eq("worker-1"), eq(leaseDuration)))
                .thenReturn(Optional.empty());

        assertEquals(Optional.empty(), service.claimNextEligible("worker-1", leaseDuration));

        verify(runStore).claimAndPrepare(isNull(), eq("worker-1"), eq(leaseDuration));
    }

    @Test
    void delegatesDirectedPreparation() {
        UUID runId = UUID.randomUUID();
        Duration leaseDuration = Duration.ofMinutes(5);
        when(runStore.claimAndPrepare(eq(runId), eq("worker-1"), eq(leaseDuration)))
                .thenReturn(Optional.empty());

        assertEquals(Optional.empty(), service.claimRequested(runId, "worker-1", leaseDuration));

        verify(runStore).claimAndPrepare(eq(runId), eq("worker-1"), eq(leaseDuration));
    }

    @Test
    void rejectsMissingDirectedRunId() {
        assertThrows(NullPointerException.class,
                () -> service.claimRequested(null, "worker-1", Duration.ofMinutes(5)));
    }
}
