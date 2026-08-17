package org.replicadb.server.job.domain;

import org.quartz.CronExpression;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.UUID;

public record JobSchedule(
        UUID jobDefinitionId,
        String cronExpression,
        String timeZone,
        boolean enabled,
        Instant createdAt,
        Instant updatedAt) {

    public JobSchedule {
        Objects.requireNonNull(jobDefinitionId, "jobDefinitionId must not be null");
        if (cronExpression == null || cronExpression.isBlank()) {
            throw new IllegalArgumentException("cronExpression must not be blank");
        }
        if (!CronExpression.isValidExpression(cronExpression)) {
            throw new IllegalArgumentException("cronExpression must be a valid Quartz cron expression");
        }
        if (timeZone == null || timeZone.isBlank()) {
            throw new IllegalArgumentException("timeZone must not be blank");
        }
        try {
            ZoneId.of(timeZone);
        } catch (DateTimeException exception) {
            throw new IllegalArgumentException("timeZone must be a valid IANA timezone", exception);
        }
    }
}
