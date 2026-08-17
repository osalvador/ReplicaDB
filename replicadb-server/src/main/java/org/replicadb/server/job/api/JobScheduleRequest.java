package org.replicadb.server.job.api;

import jakarta.validation.constraints.NotBlank;

public record JobScheduleRequest(
        @NotBlank String cronExpression,
        String timeZone,
        boolean enabled) {
}