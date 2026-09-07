package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

@Schema(description = "Recurring Quartz schedule for one job.")
public record JobScheduleRequest(
        @Schema(description = "Quartz CRON expression with seconds, minutes, hours, day, month, and weekday fields.", example = "0 0 2 * * ?")
        @NotBlank String cronExpression,
        @Schema(description = "IANA time-zone identifier. Null or blank defaults to UTC.", example = "UTC", nullable = true)
        String timeZone,
        @Schema(description = "Whether the scheduler may create runs from this schedule.", defaultValue = "true")
        boolean enabled) {
}
