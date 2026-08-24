package org.replicadb.server.security.auth;

import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Profile("api")
public class LoginAttemptService {

    private static final int MAX_ATTEMPTS = 5;
    private static final Duration WINDOW = Duration.ofMinutes(15);

    private final ConcurrentHashMap<String, Deque<Instant>> failures = new ConcurrentHashMap<>();
    private final Clock clock;

    public LoginAttemptService() {
        this(Clock.systemUTC());
    }

    LoginAttemptService(Clock clock) {
        this.clock = clock;
    }

    public synchronized void checkAllowed(String username, String remoteAddress) {
        Instant now = clock.instant();
        if (hasReachedLimit(key("user", username), now)
                || hasReachedLimit(key("addr", remoteAddress), now)) {
            throw new TooManyAttemptsException();
        }
    }

    public synchronized void recordFailure(String username, String remoteAddress) {
        Instant now = clock.instant();
        addFailure(key("user", username), now);
        addFailure(key("addr", remoteAddress), now);
    }

    public synchronized void recordSuccess(String username, String remoteAddress) {
        failures.remove(key("user", username));
        failures.remove(key("addr", remoteAddress));
    }

    private boolean hasReachedLimit(String key, Instant now) {
        Deque<Instant> attempts = failures.get(key);
        if (attempts == null) {
            return false;
        }
        purge(attempts, now);
        if (attempts.isEmpty()) {
            failures.remove(key, attempts);
        }
        return attempts.size() >= MAX_ATTEMPTS;
    }

    private void addFailure(String key, Instant now) {
        Deque<Instant> attempts = failures.computeIfAbsent(key, ignored -> new ArrayDeque<>());
        purge(attempts, now);
        attempts.addLast(now);
    }

    private static void purge(Deque<Instant> attempts, Instant now) {
        Instant cutoff = now.minus(WINDOW);
        while (!attempts.isEmpty() && attempts.peekFirst().isBefore(cutoff)) {
            attempts.removeFirst();
        }
    }

    private static String key(String type, String value) {
        return type + ":" + (value == null ? "<unknown>" : value);
    }
}
