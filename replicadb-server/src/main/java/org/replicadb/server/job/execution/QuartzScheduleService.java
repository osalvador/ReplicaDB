package org.replicadb.server.job.execution;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.replicadb.server.job.domain.JobSchedule;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;

@Service
public class QuartzScheduleService {

    private static final String GROUP = "replicadb-jobs";
    private static final String JOB_DEFINITION_ID = "jobDefinitionId";

    private final Scheduler scheduler;

    public QuartzScheduleService(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void schedule(JobSchedule jobSchedule) {
        UUID jobDefinitionId = jobSchedule.jobDefinitionId();
        if (!jobSchedule.enabled()) {
            unschedule(jobDefinitionId);
            return;
        }

        JobKey jobKey = jobKey(jobDefinitionId);
        TriggerKey triggerKey = triggerKey(jobDefinitionId);
        JobDetail jobDetail = JobBuilder.newJob(ScheduledRunTriggerJob.class)
                .withIdentity(jobKey)
                .usingJobData(JOB_DEFINITION_ID, jobDefinitionId.toString())
                .storeDurably(true)
                .build();
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .forJob(jobKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(jobSchedule.cronExpression())
                        .inTimeZone(TimeZone.getTimeZone(jobSchedule.timeZone()))
                        .withMisfireHandlingInstructionDoNothing())
                .build();

        try {
            if (scheduler.checkExists(jobKey)) {
                scheduler.rescheduleJob(triggerKey, trigger);
            } else {
                scheduler.scheduleJob(jobDetail, trigger);
            }
        } catch (SchedulerException exception) {
            throw schedulerFailure("schedule", jobDefinitionId, exception);
        }
    }

    public void unschedule(UUID jobDefinitionId) {
        try {
            if (scheduler.checkExists(jobKey(jobDefinitionId))) {
                scheduler.deleteJob(jobKey(jobDefinitionId));
            }
        } catch (SchedulerException exception) {
            throw schedulerFailure("unschedule", jobDefinitionId, exception);
        }
    }

    public Optional<Instant> nextFireTime(UUID jobDefinitionId) {
        try {
            Trigger trigger = scheduler.getTrigger(triggerKey(jobDefinitionId));
            return Optional.ofNullable(trigger)
                    .map(Trigger::getNextFireTime)
                    .map(Date::toInstant);
        } catch (SchedulerException exception) {
            throw schedulerFailure("read next fire time for", jobDefinitionId, exception);
        }
    }

    private static JobKey jobKey(UUID jobDefinitionId) {
        return new JobKey(jobDefinitionId.toString(), GROUP);
    }

    private static TriggerKey triggerKey(UUID jobDefinitionId) {
        return new TriggerKey(jobDefinitionId.toString(), GROUP);
    }

    private static IllegalStateException schedulerFailure(String operation, UUID jobDefinitionId,
                                                          SchedulerException exception) {
        return new IllegalStateException("Could not " + operation + " schedule for job definition "
                + jobDefinitionId, exception);
    }
}
