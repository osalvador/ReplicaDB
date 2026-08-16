package org.replicadb.server.job.execution;

import org.replicadb.server.job.domain.JobRunStatus;

import java.util.UUID;

public record JobRunOutcome(UUID runId, JobRunStatus status, long rowsProcessed, long durationMillis) {
}