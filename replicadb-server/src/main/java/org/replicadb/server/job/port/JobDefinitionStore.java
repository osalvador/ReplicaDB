package org.replicadb.server.job.port;

import org.replicadb.server.job.domain.JobDefinition;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface JobDefinitionStore {

    enum DeleteStatus {
        DELETED,
        NOT_FOUND
    }

    record DeleteResult(DeleteStatus status, String jobName) {
    }

    JobDefinition insert(JobDefinition definition);

    JobDefinition update(JobDefinition definition);

    DeleteResult delete(UUID id);

    Optional<JobDefinition> findById(UUID id);

    Optional<JobDefinition> findByIdForUpdate(UUID id);

    Optional<JobDefinition> findByName(String name);

    List<JobDefinition> findAll();

    List<JobDefinition> findPage(int page, int size, Set<UUID> restrictToIds);

    long count(Set<UUID> restrictToIds);
}
