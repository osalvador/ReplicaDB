package org.replicadb;

public record ReplicaTaskResult(int taskId, long rowsProcessed, long startedAtMillis, long finishedAtMillis,
                                String watermarkCandidate) {

    public ReplicaTaskResult {
        if (rowsProcessed < 0) {
            throw new IllegalArgumentException("rowsProcessed must not be negative");
        }
        if (finishedAtMillis < startedAtMillis) {
            throw new IllegalArgumentException("finishedAtMillis must not precede startedAtMillis");
        }
    }

    public long durationMillis() {
        return finishedAtMillis - startedAtMillis;
    }
}
