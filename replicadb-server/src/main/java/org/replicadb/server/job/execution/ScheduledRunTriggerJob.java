package org.replicadb.server.job.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.application.RunDispatchResult;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;

import java.util.UUID;
import java.util.Map;

@DisallowConcurrentExecution
@Profile("api")
public class ScheduledRunTriggerJob implements Job {

    private static final Logger LOG = LogManager.getLogger(ScheduledRunTriggerJob.class);
    private static final String JOB_DEFINITION_ID = "jobDefinitionId";

    @Autowired
    private JobRunStore jobRunStore;

    @Autowired
    private RunDispatchService runDispatchService;

    @Autowired
    private RunExecutionCoordinator runExecutionCoordinator;

    @Value("${replicadb.server.local-execution.enabled:true}")
    private boolean localExecutionEnabled;

    @Autowired
    private AuditService auditService;

    @Autowired
    private AuditActorResolver auditActorResolver;

    @Override
    public void execute(JobExecutionContext context) {
        JobDataMap jobData = context.getMergedJobDataMap();
        UUID jobDefinitionId = UUID.fromString(jobData.getString(JOB_DEFINITION_ID));
        LOG.info("Scheduled trigger fired for job definition {}", jobDefinitionId);

        if (jobRunStore.hasActiveRun(jobDefinitionId)) {
            LOG.info("Scheduled trigger skipped for job definition {} because a run is already active",
                    jobDefinitionId);
            return;
        }

        RunDispatchResult dispatch;
        try {
            dispatch = runDispatchService.dispatchScheduled(jobDefinitionId);
        } catch (IllegalStateException exception) {
            LOG.info("Scheduled trigger skipped for job definition {} because another run became active",
                    jobDefinitionId);
            return;
        }

        JobRun pending = dispatch.run().orElseThrow(() -> new IllegalStateException(
                "Scheduled dispatch did not return a JobRun"));
        if (localExecutionEnabled) {
            runExecutionCoordinator.submit(pending.id(), "scheduler");
        }
        auditService.record(auditActorResolver.system("scheduler"), AuditAction.RUN_TRIGGERED,
            AuditResourceType.JOB_RUN, pending.id().toString(), AuditOutcome.SUCCESS,
            Map.of("jobDefinitionId", jobDefinitionId.toString(), "trigger", "schedule"));
    }
}
