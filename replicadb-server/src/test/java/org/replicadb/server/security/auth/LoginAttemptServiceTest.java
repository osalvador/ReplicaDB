package org.replicadb.server.security.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.security.persistence.LoginAttemptRepository;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoginAttemptServiceTest {

    private LoginAttemptRepository repository;
    private LoginAttemptService service;
    private LoginAttemptReservation reservation;

    @BeforeEach
    void setUp() {
        repository = mock(LoginAttemptRepository.class);
        service = new LoginAttemptService(repository);
        reservation = new LoginAttemptReservation(UUID.randomUUID(), "user:alice", "addr:10.0.0.1");
    }

    @Test
    void returnsRepositoryReservationWhenAttemptIsAllowed() {
        when(repository.reserve("alice", "10.0.0.1")).thenReturn(Optional.of(reservation));

        assertEquals(reservation, service.checkAllowed("alice", "10.0.0.1"));
    }

    @Test
    void blocksWhenRepositoryReportsLimitReached() {
        when(repository.reserve("alice", "10.0.0.1")).thenReturn(Optional.empty());

        assertThrows(TooManyAttemptsException.class,
                () -> service.checkAllowed("alice", "10.0.0.1"));
    }

    @Test
    void delegatesFailureAndSuccessFinalization() {
        service.recordFailure(reservation);
        service.recordSuccess(reservation);

        verify(repository).recordFailure(reservation);
        verify(repository).recordSuccess(reservation);
    }

    @Test
    void propagatesRepositoryFailureSoTheDecisionFailsClosed() {
        when(repository.reserve("alice", "10.0.0.1"))
                .thenThrow(new RuntimeException("database unavailable"));

        assertThrows(RuntimeException.class,
                () -> service.checkAllowed("alice", "10.0.0.1"));
    }
}
