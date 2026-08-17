package org.replicadb.server.security.auth;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LoginAttemptServiceTest {

    @Test
    void blocksAfterFiveFailuresWithinWindow() {
        LoginAttemptService service = new LoginAttemptService();

        recordFailures(service, "alice", "10.0.0.1", 5);

        assertThrows(TooManyAttemptsException.class,
                () -> service.checkAllowed("alice", "10.0.0.1"));
    }

    @Test
    void successClearsUsernameAndAddressCounters() {
        LoginAttemptService service = new LoginAttemptService();
        recordFailures(service, "alice", "10.0.0.1", 5);

        service.recordSuccess("alice", "10.0.0.1");

        assertDoesNotThrow(() -> service.checkAllowed("alice", "10.0.0.1"));
    }

    @Test
    void ignoresFailuresOutsideTheFifteenMinuteWindow() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-17T10:00:00Z"));
        LoginAttemptService service = new LoginAttemptService(clock);
        recordFailures(service, "alice", "10.0.0.1", 5);

        clock.advance(Duration.ofMinutes(16));

        assertDoesNotThrow(() -> service.checkAllowed("alice", "10.0.0.1"));
    }

    @Test
    void addressLimitAppliesAcrossUsernames() {
        LoginAttemptService service = new LoginAttemptService();
        recordFailures(service, "alice", "10.0.0.1", 5);

        assertThrows(TooManyAttemptsException.class,
                () -> service.checkAllowed("bob", "10.0.0.1"));
    }

    @Test
    void usernameLimitAppliesAcrossAddresses() {
        LoginAttemptService service = new LoginAttemptService();
        recordFailures(service, "alice", "10.0.0.1", 5);

        assertThrows(TooManyAttemptsException.class,
                () -> service.checkAllowed("alice", "10.0.0.2"));
        assertDoesNotThrow(() -> service.checkAllowed("carol", "10.0.0.2"));
    }

    private static void recordFailures(LoginAttemptService service, String username,
                                       String remoteAddress, int count) {
        for (int attempt = 0; attempt < count; attempt++) {
            service.recordFailure(username, remoteAddress);
        }
    }

    private static final class MutableClock extends Clock {

        private Instant current;

        private MutableClock(Instant current) {
            this.current = current;
        }

        private void advance(Duration duration) {
            current = current.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return current;
        }
    }
}
