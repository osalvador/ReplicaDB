package org.replicadb.server.job.execution;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.replicadb.server.job.domain.JobSchedule;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

@Service
@Profile("api")
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
            converge(jobKey, triggerKey, jobDetail, trigger);
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

    private void converge(JobKey jobKey, TriggerKey triggerKey, JobDetail jobDetail,
                           CronTrigger trigger) throws SchedulerException {
        SchedulerException lastFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                if (scheduler.checkExists(jobKey)) {
                    Date rescheduled = scheduler.rescheduleJob(triggerKey, trigger);
                    if (rescheduled != null) {
                        return;
                    }
                    scheduler.scheduleJob(trigger);
                    return;
                }
                scheduler.scheduleJob(jobDetail, Set.of(trigger), false);
                return;
            } catch (ObjectAlreadyExistsException exception) {
                lastFailure = exception;
            }
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        throw new SchedulerException("Could not converge Quartz schedule");
    }

    private static IllegalStateException schedulerFailure(String operation, UUID jobDefinitionId,
                                                          SchedulerException exception) {
        return new IllegalStateException("Could not " + operation + " schedule for job definition "
                + jobDefinitionId, exception);
    }
}
