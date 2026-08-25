package org.replicadb.server.security.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.auth.LoginAttemptReservation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class LoginAttemptRepositoryIT {

    @Autowired
    private LoginAttemptRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE login_attempt", Map.of());
    }

    @Test
    void reservesTwoKeysAndFinalizesFailureOnlyOnce() {
        LoginAttemptReservation reservation = repository.reserve("alice", "10.0.0.1").orElseThrow();

        assertEquals(2, rowCount());
        assertTrue(repository.recordFailure(reservation));
        assertFalse(repository.recordFailure(reservation));
        assertEquals(2, rowCount());
    }

    @Test
    void blocksTheSixthAttemptWithinTheWindow() {
        for (int attempt = 0; attempt < 5; attempt++) {
            LoginAttemptReservation reservation = repository.reserve("alice", "10.0.0.1").orElseThrow();
            assertTrue(repository.recordFailure(reservation));
        }

        assertTrue(repository.reserve("alice", "10.0.0.1").isEmpty());
    }

    @Test
    void accountAndAddressLimitsAreIndependent() {
        for (int attempt = 0; attempt < 5; attempt++) {
            LoginAttemptReservation reservation = repository.reserve("alice", "10.0.0.1").orElseThrow();
            assertTrue(repository.recordFailure(reservation));
        }

        assertTrue(repository.reserve("alice", "10.0.0.2").isEmpty());
        assertTrue(repository.reserve("bob", "10.0.0.2").isPresent());
    }

    @Test
    void expiredAttemptsNoLongerCount() {
        LoginAttemptReservation reservation = repository.reserve("alice", "10.0.0.1").orElseThrow();
        assertTrue(repository.recordFailure(reservation));
        jdbcTemplate.update("UPDATE login_attempt SET attempted_at = now() - interval '16 minutes'", Map.of());

        assertEquals(2, repository.deleteExpired());
        assertTrue(repository.reserve("alice", "10.0.0.1").isPresent());
    }

    @Test
    void successClearsBothKeysAndPriorFailures() {
        for (int attempt = 0; attempt < 4; attempt++) {
            LoginAttemptReservation reservation = repository.reserve("alice", "10.0.0.1").orElseThrow();
            assertTrue(repository.recordFailure(reservation));
        }
        LoginAttemptReservation successful = repository.reserve("alice", "10.0.0.1").orElseThrow();

        assertTrue(repository.recordSuccess(successful));
        assertEquals(0, rowCount());
        assertTrue(repository.reserve("alice", "10.0.0.1").isPresent());
        assertFalse(repository.recordSuccess(successful));
    }

    @Test
    void concurrentReservationsCannotExceedTheLimit() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int attempt = 0; attempt < 10; attempt++) {
                futures.add(executor.submit(() -> repository.reserve("alice", "10.0.0.1")
                        .map(repository::recordFailure)
                        .orElse(false)));
            }

            long allowed = 0;
            for (Future<Boolean> future : futures) {
                if (future.get(10, TimeUnit.SECONDS)) {
                    allowed++;
                }
            }
            assertEquals(5, allowed);
            assertEquals(10, rowCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void databaseFailureIsNotInterpretedAsAnAllowedAttempt() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenThrow(new SQLException("database unavailable"));
        LoginAttemptRepository unavailable = new LoginAttemptRepository(new NamedParameterJdbcTemplate(dataSource));

        assertThrows(DataAccessException.class, () -> unavailable.reserve("alice", "10.0.0.1"));
    }

    private int rowCount() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM login_attempt", Map.of(), Integer.class);
        assertNotNull(count);
        return count;
    }
}
