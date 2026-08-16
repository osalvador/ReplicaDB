package org.replicadb.server.job.domain;

public enum JobRunStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCEL_REQUESTED,
    CANCELLED,
    RETRY_SCHEDULED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == CANCELLED || this == RETRY_SCHEDULED;
    }

    public static JobRunStatus fromReplicaExitCode(int exitCode) {
        return switch (exitCode) {
            case 0 -> SUCCEEDED;
            case 1 -> FAILED;
            case 2 -> CANCELLED;
            default -> throw new IllegalArgumentException("Unknown ReplicaDB exit code: " + exitCode);
        };
    }
}
