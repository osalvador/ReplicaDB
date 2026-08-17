package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobScheduleTest {

    @Test
    void acceptsAValidCronExpressionAndTimezone() {
        assertDoesNotThrow(() -> schedule("0 0 * * * ?", "Europe/Madrid"));
    }

    @Test
    void rejectsBlankCronExpression() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> schedule(" ", "UTC"));

        assertTrue(exception.getMessage().contains("cronExpression"));
    }

    @Test
    void rejectsInvalidCronExpression() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> schedule("not-a-cron", "UTC"));

        assertTrue(exception.getMessage().contains("cronExpression"));
    }

    @Test
    void rejectsInvalidTimezone() {
        assertThrows(IllegalArgumentException.class,
                () -> schedule("0 0 * * * ?", "Not/AZone"));
    }

    @Test
    void rejectsNullJobDefinitionId() {
        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> new JobSchedule(null, "0 0 * * * ?", "UTC", true,
                        Instant.now(), Instant.now()));

        assertNull(exception.getCause());
    }

    private static JobSchedule schedule(String cronExpression, String timeZone) {
        Instant now = Instant.now();
        return new JobSchedule(UUID.randomUUID(), cronExpression, timeZone, true, now, now);
    }
}
