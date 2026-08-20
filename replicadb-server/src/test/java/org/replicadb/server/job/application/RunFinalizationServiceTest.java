package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunFinalizationServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
    private final RunFinalizationService service = new RunFinalizationService(runStore);

    @Test
    void returnsFencedOutcomeWithoutInterpretingItAsSuccess() {
        UUID runId = UUID.randomUUID();
        LeaseToken token = LeaseToken.generate();
        when(runStore.markSucceeded(runId, token, 10, 20, "42"))
                .thenReturn(JobRunStore.FencedUpdateResult.FENCED);

        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
                service.markSucceeded(runId, token, 10, 20, "42"));

        verify(runStore).markSucceeded(runId, token, 10, 20, "42");
    }

    @Test
    void delegatesProgressAndFailureWithLeaseToken() {
        UUID runId = UUID.randomUUID();
        LeaseToken token = LeaseToken.generate();
        when(runStore.recordProgress(runId, token, 10, 20))
                .thenReturn(JobRunStore.FencedUpdateResult.UPDATED);
        when(runStore.markFailed(runId, token, 10, 20, "failure"))
                .thenReturn(JobRunStore.FencedUpdateResult.UPDATED);

        assertEquals(JobRunStore.FencedUpdateResult.UPDATED,
                service.recordProgress(runId, token, 10, 20));
        assertEquals(JobRunStore.FencedUpdateResult.UPDATED,
                service.markFailed(runId, token, 10, 20, "failure"));
    }

    @Test
    void rejectsNegativeExecutionValues() {
        UUID runId = UUID.randomUUID();
        LeaseToken token = LeaseToken.generate();

        assertThrows(IllegalArgumentException.class,
                () -> service.recordProgress(runId, token, -1, 20));
        assertThrows(IllegalArgumentException.class,
                () -> service.markCancelled(runId, token, 10, -1));
    }
}
