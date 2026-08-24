package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.JobRun;

import java.util.Objects;
import java.util.Optional;

public record RunDispatchResult(Optional<JobRun> run, Outcome outcome) {

    public RunDispatchResult {
        Objects.requireNonNull(run, "run must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
    }

    public boolean created() {
        return outcome == Outcome.CREATED || outcome == Outcome.RECOVERY_REPLACEMENT;
    }

    public boolean replayed() {
        return outcome == Outcome.REPLAYED;
    }

    public boolean replacementCreated() {
        return outcome == Outcome.RECOVERY_REPLACEMENT;
    }

    public enum Outcome {
        CREATED,
        REPLAYED,
        RECOVERY_REPLACEMENT,
        RECOVERY_NOOP
    }
}