package org.replicadb.server.job.api;

import org.replicadb.server.security.domain.JobPermissionType;

import java.util.Set;
import java.util.UUID;

public record JobPermissionResponse(UUID userId, String username, Set<JobPermissionType> permissions) {
}
