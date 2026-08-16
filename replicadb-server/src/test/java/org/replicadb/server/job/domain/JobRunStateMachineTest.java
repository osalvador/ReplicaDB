package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobRunStateMachineTest {

    private static final Map<JobRunStatus, Set<JobRunStatus>> LEGAL_TRANSITIONS = Map.of(
            JobRunStatus.PENDING, Set.of(JobRunStatus.RUNNING, JobRunStatus.CANCELLED),
            JobRunStatus.RUNNING, Set.of(JobRunStatus.SUCCEEDED, JobRunStatus.FAILED,
                    JobRunStatus.CANCEL_REQUESTED, JobRunStatus.CANCELLED),
            JobRunStatus.CANCEL_REQUESTED, Set.of(JobRunStatus.CANCELLED),
            JobRunStatus.FAILED, Set.of(JobRunStatus.RETRY_SCHEDULED));

    @Test
    void acceptsEveryLegalTransition() {
        LEGAL_TRANSITIONS.forEach((from, destinations) -> destinations.forEach(
                to -> assertDoesNotThrow(() -> JobRunStateMachine.assertLegalTransition(from, to))));
    }

    @Test
    void rejectsEveryOtherTransition() {
        for (JobRunStatus from : JobRunStatus.values()) {
            for (JobRunStatus to : JobRunStatus.values()) {
                if (!LEGAL_TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
                    assertThrows(IllegalStateException.class,
                            () -> JobRunStateMachine.assertLegalTransition(from, to),
                            from + " -> " + to);
                }
            }
        }
    }
}