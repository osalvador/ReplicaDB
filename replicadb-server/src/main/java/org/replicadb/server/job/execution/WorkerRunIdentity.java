package org.replicadb.server.job.execution;

import java.util.UUID;

public record WorkerRunIdentity(String value) {

    public WorkerRunIdentity {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("worker identity must not be blank");
        }
    }

    public static WorkerRunIdentity resolve(String configuredIdentity) {
        if (configuredIdentity == null || configuredIdentity.isBlank()) {
            return new WorkerRunIdentity("worker-" + UUID.randomUUID());
        }
        return new WorkerRunIdentity(configuredIdentity);
    }
}