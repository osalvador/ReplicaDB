package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobRunStatusTest {

    @Test
    void identifiesTerminalStatuses() {
        assertTrue(JobRunStatus.SUCCEEDED.isTerminal());
        assertTrue(JobRunStatus.CANCELLED.isTerminal());
        assertTrue(JobRunStatus.RETRY_SCHEDULED.isTerminal());
        assertFalse(JobRunStatus.PENDING.isTerminal());
        assertFalse(JobRunStatus.RUNNING.isTerminal());
        assertFalse(JobRunStatus.FAILED.isTerminal());
        assertFalse(JobRunStatus.CANCEL_REQUESTED.isTerminal());
    }

    @Test
    void mapsReplicaDbExitCodes() {
        assertEquals(JobRunStatus.SUCCEEDED, JobRunStatus.fromReplicaExitCode(0));
        assertEquals(JobRunStatus.FAILED, JobRunStatus.fromReplicaExitCode(1));
        assertEquals(JobRunStatus.CANCELLED, JobRunStatus.fromReplicaExitCode(2));
    }

    @Test
    void rejectsUnknownReplicaDbExitCode() {
        assertThrows(IllegalArgumentException.class, () -> JobRunStatus.fromReplicaExitCode(99));
    }
}