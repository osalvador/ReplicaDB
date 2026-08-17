package org.replicadb.server.job.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.domain.JobSchedule;
import org.replicadb.server.job.persistence.JobScheduleRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ScheduleReconciler implements ApplicationRunner {

    private static final Logger LOG = LogManager.getLogger(ScheduleReconciler.class);

    private final JobScheduleRepository jobScheduleRepository;
    private final QuartzScheduleService quartzScheduleService;

    public ScheduleReconciler(JobScheduleRepository jobScheduleRepository,
                              QuartzScheduleService quartzScheduleService) {
        this.jobScheduleRepository = jobScheduleRepository;
        this.quartzScheduleService = quartzScheduleService;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<JobSchedule> schedules = jobScheduleRepository.findAllEnabled();
        int registered = 0;
        for (JobSchedule schedule : schedules) {
            try {
                quartzScheduleService.schedule(schedule);
                registered++;
            } catch (RuntimeException exception) {
                LOG.warn("Could not reconcile schedule for job definition {}",
                        schedule.jobDefinitionId(), exception);
            }
        }
        LOG.info("Reconciled {} of {} enabled job schedules", registered, schedules.size());
    }
}