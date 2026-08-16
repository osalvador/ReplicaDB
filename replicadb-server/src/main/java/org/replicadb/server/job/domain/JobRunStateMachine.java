package org.replicadb.server.job.domain;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class JobRunStateMachine {

    private static final Map<JobRunStatus, Set<JobRunStatus>> LEGAL_TRANSITIONS = Map.of(
            JobRunStatus.PENDING, Set.of(JobRunStatus.RUNNING, JobRunStatus.CANCELLED),
            JobRunStatus.RUNNING, Set.of(JobRunStatus.SUCCEEDED, JobRunStatus.FAILED,
                    JobRunStatus.CANCEL_REQUESTED, JobRunStatus.CANCELLED),
            JobRunStatus.CANCEL_REQUESTED, Set.of(JobRunStatus.CANCELLED),
            JobRunStatus.FAILED, Set.of(JobRunStatus.RETRY_SCHEDULED));

    private JobRunStateMachine() {
    }

    public static void assertLegalTransition(JobRunStatus from, JobRunStatus to) {
        Objects.requireNonNull(from, "from status must not be null");
        Objects.requireNonNull(to, "to status must not be null");

        if (!LEGAL_TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalStateException("Illegal JobRun transition: " + from + " -> " + to);
        }
    }
}
