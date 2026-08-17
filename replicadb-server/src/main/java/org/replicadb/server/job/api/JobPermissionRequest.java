package org.replicadb.server.job.api;

import jakarta.validation.constraints.NotNull;
import org.replicadb.server.security.domain.JobPermissionType;

import java.util.Set;

public record JobPermissionRequest(@NotNull Set<JobPermissionType> permissions) {
}
