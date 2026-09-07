package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.job.domain.JobSchedule;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Persisted job schedule and its currently computed next fire time.")
public record JobScheduleResponse(
    @Schema(description = "Scheduled job definition identifier.", format = "uuid") UUID jobDefinitionId,
    @Schema(description = "Validated Quartz CRON expression.") String cronExpression,
    @Schema(description = "IANA time-zone identifier used by Quartz.") String timeZone,
    @Schema(description = "Whether the schedule is enabled.") boolean enabled,
    @Schema(description = "Creation timestamp in UTC.", format = "date-time") Instant createdAt,
    @Schema(description = "Last update timestamp in UTC.", format = "date-time") Instant updatedAt,
    @Schema(description = "Next fire time computed by the current scheduler, or null when disabled or unscheduled.", format = "date-time", nullable = true) Instant nextFireTime) {

    public static JobScheduleResponse from(JobSchedule schedule, Instant nextFireTime) {
        return new JobScheduleResponse(
                schedule.jobDefinitionId(), schedule.cronExpression(), schedule.timeZone(), schedule.enabled(),
                schedule.createdAt(), schedule.updatedAt(), nextFireTime);
    }
}
