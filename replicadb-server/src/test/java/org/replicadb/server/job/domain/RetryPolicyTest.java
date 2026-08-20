package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    @Test
    void appliesLockedDefaultsForEachMode() {
        RetryPolicy complete = RetryPolicy.defaultsFor(ReplicationMode.COMPLETE);
        RetryPolicy atomic = RetryPolicy.defaultsFor(ReplicationMode.COMPLETE_ATOMIC);
        RetryPolicy incremental = RetryPolicy.defaultsFor(ReplicationMode.INCREMENTAL);

        assertEquals(3, complete.maxAttempts());
        assertEquals(60, complete.retryBackoffSeconds());
        assertFalse(complete.automaticRetryEnabled());
        assertTrue(atomic.automaticRetryEnabled());
        assertTrue(incremental.automaticRetryEnabled());
    }

    @Test
    void acceptsValidationBoundaries() {
        assertDoesNotThrow(() -> new RetryPolicy(1, 0, false));
        assertDoesNotThrow(() -> new RetryPolicy(Integer.MAX_VALUE, Long.MAX_VALUE, true));
    }

    @Test
    void rejectsInvalidAttemptsAndBackoff() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, 0, false));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, -1, false));
    }

    @Test
    void rejectsNullModeWhenDerivingDefaults() {
        assertThrows(NullPointerException.class, () -> RetryPolicy.defaultsFor(null));
    }
}
