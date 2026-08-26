package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContentionBackoffTest {

    @Test
    void increasesToTheConfiguredMaximum() {
        AtomicLong now = new AtomicLong();
        ContentionBackoff backoff = new ContentionBackoff(true,
                Duration.ofMillis(100), Duration.ofMillis(400), Duration.ofSeconds(10), now::get);

        assertEquals(Duration.ZERO, backoff.currentDelay());
        backoff.recordContention();
        assertEquals(Duration.ofMillis(100), backoff.currentDelay());
        backoff.recordContention();
        assertEquals(Duration.ofMillis(200), backoff.currentDelay());
        backoff.recordContention();
        assertEquals(Duration.ofMillis(400), backoff.currentDelay());
        backoff.recordContention();
        assertEquals(Duration.ofMillis(400), backoff.currentDelay());
    }

    @Test
    void decaysByTheConfiguredHalfLife() {
        AtomicLong now = new AtomicLong();
        ContentionBackoff backoff = new ContentionBackoff(true,
                Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(10), now::get);

        backoff.recordContention();
        now.set(Duration.ofSeconds(10).toNanos());

        Duration decayed = backoff.currentDelay();
        assertTrue(decayed.compareTo(Duration.ZERO) > 0);
        assertTrue(decayed.compareTo(Duration.ofMillis(100)) < 0);
    }

    @Test
    void resetAndUncontendedWorkReduceTheDelay() {
        AtomicLong now = new AtomicLong();
        ContentionBackoff backoff = new ContentionBackoff(true,
                Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(10), now::get);

        backoff.recordContention();
        backoff.recordContention();
        Duration beforeUncontended = backoff.currentDelay();
        backoff.recordUncontendedWork();
        assertTrue(backoff.currentDelay().compareTo(beforeUncontended) < 0);
        backoff.reset();
        assertEquals(Duration.ZERO, backoff.currentDelay());
    }

    @Test
    void disabledBackoffIsInert() {
        AtomicLong now = new AtomicLong();
        ContentionBackoff backoff = new ContentionBackoff(false,
                Duration.ZERO, Duration.ZERO, Duration.ZERO, now::get);

        backoff.recordContention();
        assertEquals(Duration.ZERO, backoff.currentDelay());
    }
}