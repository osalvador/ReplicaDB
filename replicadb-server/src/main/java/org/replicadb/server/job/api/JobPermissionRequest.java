package org.replicadb.server.job.api;

import jakarta.validation.constraints.NotEmpty;
import org.replicadb.server.security.domain.JobPermissionType;

import java.util.Set;

public record JobPermissionRequest(@NotEmpty Set<JobPermissionType> permissions) {
}
