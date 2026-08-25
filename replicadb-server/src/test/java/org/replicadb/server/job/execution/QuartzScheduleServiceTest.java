package org.replicadb.server.job.execution;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.CronTrigger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.replicadb.server.job.domain.JobSchedule;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuartzScheduleServiceTest {

    private Scheduler scheduler;
    private QuartzScheduleService service;

    @BeforeEach
    void setUp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("org.quartz.scheduler.instanceName", "QuartzScheduleServiceTest-"
                + UUID.randomUUID());
        properties.setProperty("org.quartz.threadPool.threadCount", "1");
        properties.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
        scheduler = new StdSchedulerFactory(properties).getScheduler();
        scheduler.start();
        service = new QuartzScheduleService(scheduler);
    }

    @AfterEach
    void tearDown() throws Exception {
        scheduler.shutdown(true);
    }

    @Test
    void registersOneJobAndTriggerWithTimezoneAndDoNothingMisfirePolicy() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();

        service.schedule(schedule(jobDefinitionId, "0 0 1 1 1 ?", "Europe/Madrid", true));

        assertEquals(Set.of(jobKey(jobDefinitionId)), scheduler.getJobKeys(
                GroupMatcher.jobGroupEquals("replicadb-jobs")));
        assertEquals(Set.of(triggerKey(jobDefinitionId)), scheduler.getTriggerKeys(
                GroupMatcher.triggerGroupEquals("replicadb-jobs")));
        CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey(jobDefinitionId));
        assertEquals("Europe/Madrid", trigger.getTimeZone().getID());
        assertEquals(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING, trigger.getMisfireInstruction());
        assertTrue(service.nextFireTime(jobDefinitionId).isPresent());
    }

    @Test
    void reschedulesAnExistingJobWhenTheCronExpressionChanges() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        service.schedule(schedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", true));
        Instant firstFireTime = service.nextFireTime(jobDefinitionId).orElseThrow();

        service.schedule(schedule(jobDefinitionId, "0 0 1 2 1 ?", "UTC", true));

        assertNotEquals(firstFireTime, service.nextFireTime(jobDefinitionId).orElseThrow());
        assertEquals(1, scheduler.getJobKeys(GroupMatcher.jobGroupEquals("replicadb-jobs")).size());
        assertEquals(1, scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals("replicadb-jobs")).size());
    }

    @Test
    void repeatedRegistrationWithTheSameCronExpressionKeepsOneJobAndTrigger() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        JobSchedule schedule = schedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", true);

        service.schedule(schedule);
        service.schedule(schedule);

        assertEquals(1, scheduler.getJobKeys(GroupMatcher.jobGroupEquals("replicadb-jobs")).size());
        assertEquals(1, scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals("replicadb-jobs")).size());
    }

    @Test
    void concurrentRegistrationConvergesToOneJobAndTrigger() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        JobSchedule schedule = schedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", true);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit((java.util.concurrent.Callable<Void>) () -> {
                registerAfter(start, ready, schedule);
                return null;
            });
            var second = executor.submit((java.util.concurrent.Callable<Void>) () -> {
                registerAfter(start, ready, schedule);
                return null;
            });
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(1, scheduler.getJobKeys(GroupMatcher.jobGroupEquals("replicadb-jobs")).size());
        assertEquals(1, scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals("replicadb-jobs")).size());
    }

    @Test
    void disabledSchedulesAreNotRegistered() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        service.schedule(schedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", true));

        service.schedule(schedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", false));

        assertFalse(scheduler.checkExists(jobKey(jobDefinitionId)));
        assertFalse(scheduler.checkExists(triggerKey(jobDefinitionId)));
    }

    @Test
    void unscheduleRemovesAnExistingJobAndIsSafeForUnknownJobs() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        service.schedule(schedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", true));

        service.unschedule(jobDefinitionId);
        service.unschedule(UUID.randomUUID());

        assertFalse(scheduler.checkExists(jobKey(jobDefinitionId)));
        assertTrue(service.nextFireTime(jobDefinitionId).isEmpty());
    }

    @Test
    void returnsEmptyForAnUnknownJobDefinition() {
        assertTrue(service.nextFireTime(UUID.randomUUID()).isEmpty());
    }

    private static JobSchedule schedule(UUID jobDefinitionId, String cronExpression,
                                        String timeZone, boolean enabled) {
        Instant now = Instant.now();
        return new JobSchedule(jobDefinitionId, cronExpression, timeZone, enabled, now, now);
    }

    private void registerAfter(CountDownLatch start, CountDownLatch ready,
                               JobSchedule schedule) throws Exception {
        ready.countDown();
        assertTrue(start.await(5, TimeUnit.SECONDS));
        service.schedule(schedule);
    }

    private static JobKey jobKey(UUID jobDefinitionId) {
        return new JobKey(jobDefinitionId.toString(), "replicadb-jobs");
    }

    private static TriggerKey triggerKey(UUID jobDefinitionId) {
        return new TriggerKey(jobDefinitionId.toString(), "replicadb-jobs");
    }
}
