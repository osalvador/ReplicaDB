package org.replicadb.server.job.domain;

import java.util.Objects;

public record ClaimedRunPreparation(
        JobRun run,
        JobDefinition definition,
        ManagedDataSource sourceDataSource,
        ManagedDataSource sinkDataSource) {

    public ClaimedRunPreparation {
        Objects.requireNonNull(run, "run must not be null");
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(sourceDataSource, "sourceDataSource must not be null");
        Objects.requireNonNull(sinkDataSource, "sinkDataSource must not be null");
        if (!Objects.equals(run.jobDefinitionId(), definition.id())) {
            throw new IllegalArgumentException("run and definition must refer to the same job");
        }
        if (!Objects.equals(definition.sourceDatasourceId(), sourceDataSource.id())) {
            throw new IllegalArgumentException("source datasource does not match the definition");
        }
        if (!Objects.equals(definition.sinkDatasourceId(), sinkDataSource.id())) {
            throw new IllegalArgumentException("sink datasource does not match the definition");
        }
        if (!Objects.equals(run.resolvedSourceDatasourceId(), sourceDataSource.id())
                || !Objects.equals(run.resolvedSinkDatasourceId(), sinkDataSource.id())
                || run.datasourcesResolvedAt() == null) {
            throw new IllegalArgumentException("run must contain datasource resolution metadata");
        }
    }
}
