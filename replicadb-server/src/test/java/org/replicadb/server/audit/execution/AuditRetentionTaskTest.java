package org.replicadb.server.audit.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.persistence.AuditEventRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuditRetentionTaskTest {

    @Test
    void purgesWithConfiguredRetentionDaysAndReturnsDeletedCount() {
        AuditEventRepository repository = mock(AuditEventRepository.class);
        when(repository.deleteOlderThan(365)).thenReturn(3);
        AuditRetentionTask task = new AuditRetentionTask(repository, 365);

        assertEquals(3, task.purgeExpired());
        verify(repository).deleteOlderThan(365);
    }

    @Test
    void scheduledEntryPointUsesTheSameCleanupOperation() {
        AuditEventRepository repository = mock(AuditEventRepository.class);
        AuditRetentionTask task = new AuditRetentionTask(repository, 30);

        task.purgeExpiredOnSchedule();

        verify(repository).deleteOlderThan(30);
    }

    @Test
    void rejectsZeroRetentionDays() {
        assertThrows(IllegalArgumentException.class,
                () -> new AuditRetentionTask(mock(AuditEventRepository.class), 0));
    }

    @Test
    void rejectsNegativeRetentionDays() {
        assertThrows(IllegalArgumentException.class,
                () -> new AuditRetentionTask(mock(AuditEventRepository.class), -1));
    }
}
