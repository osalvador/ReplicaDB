package org.replicadb.server.job.execution;

import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Map;
import java.util.function.Consumer;

@Service
public class JobExecutionService {

    private final JobRunRepository jobRunRepository;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final JobDefinitionEnvResolver environmentResolver;
    private final ToolOptionsArgsBuilder argumentsBuilder;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public JobExecutionService(JobRunRepository jobRunRepository,
                               JobDefinitionRepository jobDefinitionRepository,
                               JobDefinitionEnvResolver environmentResolver,
                               ToolOptionsArgsBuilder argumentsBuilder,
                               AuditService auditService,
                               AuditActorResolver auditActorResolver) {
        this.jobRunRepository = jobRunRepository;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.environmentResolver = environmentResolver;
        this.argumentsBuilder = argumentsBuilder;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    public Optional<JobRunOutcome> executeNextPending(String executorIdentity) {
        Optional<JobRun> claimed = jobRunRepository.claimNextPending(executorIdentity,
                java.time.Duration.ofMinutes(5));
        return claimed.map(run -> executeClaimedRun(run, options -> { }));
    }

    public JobRunOutcome executeClaimedRun(JobRun run, Consumer<ToolOptions> onStarted) {
        ToolOptions options = null;
        try {
            JobDefinition definition = jobDefinitionRepository.findById(run.jobDefinitionId())
                    .orElseThrow(() -> new IllegalStateException(
                            "JobDefinition not found for JobRun " + run.id()));
            String previousWatermark = jobRunRepository.findLastCommittedWatermark(definition.id())
                    .orElse(definition.initialWatermarkValue());
            String[] arguments = argumentsBuilder.build(definition, previousWatermark,
                    environmentResolver::resolve);
            options = new ToolOptions(arguments);
                onStarted.accept(options);

            int exitCode = ReplicaDB.processReplica(options);
            JobRunStatus status = JobRunStatus.fromReplicaExitCode(exitCode);
            long rowsProcessed = options.getExecutionContext().getRowsProcessed();
            long durationMillis = options.getExecutionContext().getDurationMillis();
            if (status == JobRunStatus.SUCCEEDED) {
                jobRunRepository.markSucceeded(run.id(), rowsProcessed, durationMillis,
                        options.getExecutionContext().getWatermarkCandidate());
                recordTerminalOutcome(run, status, rowsProcessed, durationMillis, null);
            } else if (status == JobRunStatus.CANCELLED) {
                jobRunRepository.markCancelled(run.id(), rowsProcessed, durationMillis);
                recordTerminalOutcome(run, status, rowsProcessed, durationMillis, null);
            } else {
                String errorMessage = redactedFailureMessage("ReplicaDB execution failed for run " + run.id());
                jobRunRepository.markFailed(run.id(), rowsProcessed, durationMillis, errorMessage);
                recordTerminalOutcome(run, JobRunStatus.FAILED, rowsProcessed, durationMillis, errorMessage);
            }
            return new JobRunOutcome(run.id(), status, rowsProcessed, durationMillis);
        } catch (Exception exception) {
            long rowsProcessed = options == null ? 0 : options.getExecutionContext().getRowsProcessed();
            long durationMillis = options == null ? 0 : options.getExecutionContext().getDurationMillis();
            String message = CredentialRedactor.redactMessage(exception.getMessage());
            if (message == null || message.isBlank()) {
                message = exception.getClass().getSimpleName();
            }
            jobRunRepository.markFailed(run.id(), rowsProcessed, durationMillis, message);
            recordTerminalOutcome(run, JobRunStatus.FAILED, rowsProcessed, durationMillis, message);
            return new JobRunOutcome(run.id(), JobRunStatus.FAILED, rowsProcessed, durationMillis);
        }
    }

    private void recordTerminalOutcome(JobRun run, JobRunStatus status, long rowsProcessed,
                                       long durationMillis, String errorMessage) {
        AuditAction action = switch (status) {
            case SUCCEEDED -> AuditAction.RUN_SUCCEEDED;
            case CANCELLED -> AuditAction.RUN_CANCELLED;
            default -> AuditAction.RUN_FAILED;
        };
        AuditOutcome outcome = status == JobRunStatus.FAILED ? AuditOutcome.FAILURE : AuditOutcome.SUCCESS;
        Map<String, String> detail = errorMessage == null
                ? Map.of("rowsProcessed", Long.toString(rowsProcessed),
                "durationMillis", Long.toString(durationMillis))
                : Map.of("rowsProcessed", Long.toString(rowsProcessed),
                "durationMillis", Long.toString(durationMillis), "errorMessage", errorMessage);
        auditService.record(auditActorResolver.system(run.executorIdentity()), action,
                AuditResourceType.JOB_RUN, run.id().toString(), outcome, detail);
    }

    private static String redactedFailureMessage(String message) {
        String redacted = CredentialRedactor.redactMessage(message);
        if (redacted == null || redacted.isBlank()) {
            return "Job execution failed";
        }
        return redacted;
    }
}
