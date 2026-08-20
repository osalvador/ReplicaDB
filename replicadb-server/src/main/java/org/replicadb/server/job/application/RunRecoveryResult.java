package org.replicadb.server.job.application;

import org.replicadb.server.job.domain.JobRun;

import java.util.Objects;
import java.util.Optional;

public record RunRecoveryResult(Optional<JobRun> abandonedRun, Optional<JobRun> replacementRun) {

    public RunRecoveryResult {
        Objects.requireNonNull(abandonedRun, "abandonedRun must not be null");
        Objects.requireNonNull(replacementRun, "replacementRun must not be null");
    }

    public boolean replacementCreated() {
        return replacementRun.isPresent();
    }
}
