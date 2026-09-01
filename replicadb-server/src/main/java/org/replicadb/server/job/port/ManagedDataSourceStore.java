package org.replicadb.server.job.port;

import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface ManagedDataSourceStore {

    ManagedDataSource insert(ManagedDataSource dataSource);

    ManagedDataSource update(ManagedDataSource dataSource);

    Optional<ManagedDataSource> findById(UUID id);

    Optional<ManagedDataSource> findByIdForUpdate(UUID id);

    Optional<ManagedDataSourceSummary> findSummaryById(UUID id);

    Optional<ManagedDataSource> findByName(String name);

    List<ManagedDataSourceSummary> findPage(int page, int size,
                                            Set<UUID> restrictToIds,
                                            Set<ConnectorType> restrictToTypes);

    long count(Set<UUID> restrictToIds, Set<ConnectorType> restrictToTypes);

    DeleteResult delete(UUID id);

    long countJobReferences(UUID id);

    enum DeleteResult {
        DELETED,
        NOT_FOUND,
        REFERENCED
    }
}
