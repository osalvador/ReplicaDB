package org.replicadb.server.job.execution;

import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.ResolvedDataSource;

import java.util.Objects;

public record ResolvedJobDefinition(
        JobDefinition definition,
        ResolvedDataSource sourceDataSource,
        ResolvedDataSource sinkDataSource) {

    public ResolvedJobDefinition {
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(sourceDataSource, "sourceDataSource must not be null");
        Objects.requireNonNull(sinkDataSource, "sinkDataSource must not be null");
        if (!sourceDataSource.id().equals(definition.sourceDatasourceId())) {
            throw new IllegalArgumentException("source datasource does not match the job definition");
        }
        if (!sinkDataSource.id().equals(definition.sinkDatasourceId())) {
            throw new IllegalArgumentException("sink datasource does not match the job definition");
        }
    }
}
