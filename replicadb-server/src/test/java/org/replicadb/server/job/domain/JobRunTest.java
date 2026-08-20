package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobRunTest {

    @Test
    void acceptsAValidRun() {
        Instant createdAt = Instant.now();
        JobRun run = new JobRun(
            UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.PENDING, 1,
            null, null, null, createdAt, null, null,
            null, null, null, null, null);

        assertNotNull(run.availableAt());
        assertEquals(createdAt, run.availableAt());
        assertDoesNotThrow(() -> run);
    }

    @Test
    void rejectsNonPositiveAttempt() {
        assertThrows(IllegalArgumentException.class, () -> new JobRun(
                UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.PENDING, 0,
                null, null, null, Instant.now(), null, null,
                null, null, null, null, null));
    }

    @Test
    void rejectsMissingAvailableAt() {
        assertThrows(NullPointerException.class, () -> new JobRun(
                UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.PENDING, 1,
                null, null, null, null, null, null,
                null, null, null, null, null, null, null));
    }
}
