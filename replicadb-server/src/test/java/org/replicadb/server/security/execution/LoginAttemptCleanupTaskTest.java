package org.replicadb.server.security.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.security.persistence.LoginAttemptRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoginAttemptCleanupTaskTest {

    @Test
    void purgesExpiredRowsUsingRepository() {
        LoginAttemptRepository repository = mock(LoginAttemptRepository.class);
        when(repository.deleteExpired()).thenReturn(4);
        LoginAttemptCleanupTask task = new LoginAttemptCleanupTask(repository);

        assertEquals(4, task.purgeExpired());
        verify(repository).deleteExpired();
    }
}
