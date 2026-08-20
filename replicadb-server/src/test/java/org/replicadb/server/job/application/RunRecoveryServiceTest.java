package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunRecoveryServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
    private final RunRecoveryService service = new RunRecoveryService(runStore);

    @Test
    void delegatesRecoveryAndPreservesReplacementResult() {
        UUID runId = UUID.randomUUID();
        RunRecoveryResult result = new RunRecoveryResult(Optional.of(run()), Optional.empty());
        when(runStore.recoverExpiredRun(runId)).thenReturn(result);

        assertSame(result, service.recoverExpiredRun(runId));

        verify(runStore).recoverExpiredRun(runId);
    }

    @Test
    void rejectsMissingRunId() {
        assertThrows(NullPointerException.class, () -> service.recoverExpiredRun(null));
    }

    private static JobRun run() {
        Instant createdAt = Instant.now();
        return new JobRun(UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.RUNNING, 1,
                "worker", createdAt.plusSeconds(1), createdAt, createdAt, createdAt,
                null, 0L, 1L, null, null, null);
    }
}
