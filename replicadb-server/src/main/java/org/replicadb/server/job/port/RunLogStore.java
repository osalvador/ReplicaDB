package org.replicadb.server.job.port;

import org.replicadb.server.job.domain.RunLog;

import java.util.Optional;
import java.util.UUID;

public interface RunLogStore {

    void replaceTerminal(RunLog runLog);

    Optional<RunLog> findByRunId(UUID runId);
}
