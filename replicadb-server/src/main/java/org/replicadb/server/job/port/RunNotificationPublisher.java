package org.replicadb.server.job.port;

import java.util.UUID;

public interface RunNotificationPublisher {

    String RUN_CHANNEL = "replicadb_runs";
    String CONTROL_CHANNEL = "replicadb_run_control";

    void publishRun(UUID runId);

    void publishCancellation(UUID runId);
}