package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.config.RunLogRetentionConfiguration;
import org.replicadb.server.job.persistence.RunLogRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunLogRetentionTaskTest {

    @Test
    void usesConfiguredRetentionAndBatchSize() {
        RunLogRepository repository = mock(RunLogRepository.class);
        when(repository.deleteOlderThan(30, 25)).thenReturn(7);
        RunLogRetentionTask task = new RunLogRetentionTask(repository,
                new RunLogRetentionConfiguration(30, 25));

        assertEquals(7, task.purgeExpired());
        verify(repository).deleteOlderThan(30, 25);
    }

    @Test
    void rejectsInvalidConfiguration() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RunLogRetentionConfiguration(0, 1));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RunLogRetentionConfiguration(1, 0));
    }
}
