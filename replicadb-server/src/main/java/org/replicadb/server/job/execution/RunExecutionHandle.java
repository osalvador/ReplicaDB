package org.replicadb.server.job.execution;

import org.replicadb.cli.ToolOptions;
import org.replicadb.execution.ReplicationExecutionContext;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.LeaseToken;

import java.util.Objects;
import java.util.UUID;

public final class RunExecutionHandle {

    private final JobRun claimedRun;
    private final ToolOptions toolOptions;
    private final ReplicationExecutionContext cancellationContext;

    public RunExecutionHandle(JobRun claimedRun, ToolOptions toolOptions) {
        this.claimedRun = Objects.requireNonNull(claimedRun, "claimedRun must not be null");
        this.toolOptions = Objects.requireNonNull(toolOptions, "toolOptions must not be null");
        this.cancellationContext = toolOptions.getExecutionContext();
        if (claimedRun.leaseToken() == null) {
            throw new IllegalArgumentException("claimedRun must have a lease token");
        }
    }

    public UUID runId() {
        return claimedRun.id();
    }

    public LeaseToken leaseToken() {
        return claimedRun.leaseToken();
    }

    public ToolOptions toolOptions() {
        return toolOptions;
    }

    public ReplicationExecutionContext cancellationContext() {
        return cancellationContext;
    }

    public void requestCancellation() {
        cancellationContext.requestCancellation();
    }
}