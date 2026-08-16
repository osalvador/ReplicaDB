package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.persistence.RunTriggerIdempotencyRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyCleanupTaskTest {

    @Test
    void purgesExpiredRowsThroughTheRepository() {
        RunTriggerIdempotencyRepository repository = mock(RunTriggerIdempotencyRepository.class);
        when(repository.deleteExpired()).thenReturn(3);
        IdempotencyCleanupTask task = new IdempotencyCleanupTask(repository);

        assertEquals(3, task.purgeExpired());
        verify(repository).deleteExpired();
    }

    @Test
    void scheduledEntryPointUsesTheSameCleanupOperation() {
        RunTriggerIdempotencyRepository repository = mock(RunTriggerIdempotencyRepository.class);
        IdempotencyCleanupTask task = new IdempotencyCleanupTask(repository);

        task.purgeExpiredOnSchedule();

        verify(repository).deleteExpired();
    }
}
