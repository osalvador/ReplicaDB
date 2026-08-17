package org.replicadb.server.job.api;

import org.replicadb.server.job.domain.JobSchedule;

import java.time.Instant;
import java.util.UUID;

public record JobScheduleResponse(
        UUID jobDefinitionId,
        String cronExpression,
        String timeZone,
        boolean enabled,
        Instant createdAt,
        Instant updatedAt,
        Instant nextFireTime) {

    public static JobScheduleResponse from(JobSchedule schedule, Instant nextFireTime) {
        return new JobScheduleResponse(
                schedule.jobDefinitionId(), schedule.cronExpression(), schedule.timeZone(), schedule.enabled(),
                schedule.createdAt(), schedule.updatedAt(), nextFireTime);
    }
}