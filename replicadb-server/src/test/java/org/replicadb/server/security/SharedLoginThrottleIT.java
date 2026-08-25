package org.replicadb.server.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.auth.LoginAttemptReservation;
import org.replicadb.server.security.auth.LoginAttemptService;
import org.replicadb.server.security.auth.TooManyAttemptsException;
import org.replicadb.server.security.persistence.LoginAttemptRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class SharedLoginThrottleIT {

    @Autowired
    private LoginAttemptRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE login_attempt", Map.of());
    }

    @Test
    void fiveFailuresAcrossIndependentServicesBlockTheNextAttempt() {
        LoginAttemptService firstApi = new LoginAttemptService(repository);
        LoginAttemptService secondApi = new LoginAttemptService(new LoginAttemptRepository(jdbcTemplate));

        for (int attempt = 0; attempt < 5; attempt++) {
            LoginAttemptService service = attempt % 2 == 0 ? firstApi : secondApi;
            LoginAttemptReservation reservation = service.checkAllowed("alice", "10.0.0.1");
            service.recordFailure(reservation);
        }

        assertThrows(TooManyAttemptsException.class,
                () -> secondApi.checkAllowed("alice", "10.0.0.1"));
    }

    @Test
    void accountAndAddressLimitsAreSharedAcrossIndependentServices() {
        LoginAttemptService firstApi = new LoginAttemptService(repository);
        LoginAttemptService secondApi = new LoginAttemptService(new LoginAttemptRepository(jdbcTemplate));

        for (int attempt = 0; attempt < 5; attempt++) {
            LoginAttemptReservation reservation = firstApi.checkAllowed("alice", "10.0.0.1");
            firstApi.recordFailure(reservation);
        }

        assertThrows(TooManyAttemptsException.class,
                () -> secondApi.checkAllowed("alice", "10.0.0.2"));
        assertDoesNotThrow(() -> secondApi.checkAllowed("bob", "10.0.0.2"));
    }

    @Test
    void successfulAuthenticationClearsFailuresForBothKeys() {
        LoginAttemptService firstApi = new LoginAttemptService(repository);
        LoginAttemptService secondApi = new LoginAttemptService(new LoginAttemptRepository(jdbcTemplate));
        for (int attempt = 0; attempt < 4; attempt++) {
            LoginAttemptReservation reservation = firstApi.checkAllowed("alice", "10.0.0.1");
            firstApi.recordFailure(reservation);
        }

        LoginAttemptReservation successful = secondApi.checkAllowed("alice", "10.0.0.1");
        secondApi.recordSuccess(successful);

        assertTrue(firstApi.checkAllowed("alice", "10.0.0.1") != null);
    }

    @Test
    void expiredFailuresAreIgnoredByTheOtherServiceInstance() {
        LoginAttemptService firstApi = new LoginAttemptService(repository);
        LoginAttemptService secondApi = new LoginAttemptService(new LoginAttemptRepository(jdbcTemplate));
        for (int attempt = 0; attempt < 5; attempt++) {
            LoginAttemptReservation reservation = firstApi.checkAllowed("alice", "10.0.0.1");
            firstApi.recordFailure(reservation);
        }
        jdbcTemplate.update("UPDATE login_attempt SET attempted_at = now() - interval '16 minutes'", Map.of());

        assertDoesNotThrow(() -> secondApi.checkAllowed("alice", "10.0.0.1"));
    }
}
