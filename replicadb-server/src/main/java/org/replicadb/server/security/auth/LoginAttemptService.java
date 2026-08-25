package org.replicadb.server.security.auth;

import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import org.replicadb.server.security.persistence.LoginAttemptRepository;

import java.util.Objects;
import java.util.Optional;

@Service
@Profile("api")
public class LoginAttemptService {

    private final LoginAttemptRepository repository;

    public LoginAttemptService(LoginAttemptRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
    }

    public LoginAttemptReservation checkAllowed(String username, String remoteAddress) {
        Optional<LoginAttemptReservation> reservation = repository.reserve(username, remoteAddress);
        if (reservation.isEmpty()) {
            throw new TooManyAttemptsException();
        }
        return reservation.orElseThrow();
    }

    public void recordFailure(LoginAttemptReservation reservation) {
        repository.recordFailure(Objects.requireNonNull(reservation, "reservation must not be null"));
    }

    public void recordSuccess(LoginAttemptReservation reservation) {
        repository.recordSuccess(Objects.requireNonNull(reservation, "reservation must not be null"));
    }

}
