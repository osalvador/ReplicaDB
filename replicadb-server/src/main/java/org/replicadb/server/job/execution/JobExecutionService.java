package org.replicadb.server.job.execution;

import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.CredentialRedactor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.application.RunFinalizationService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Map;
import java.util.function.Consumer;

@Service
public class JobExecutionService {

    private static final Logger LOG = LogManager.getLogger(JobExecutionService.class);

    private final JobRunStore jobRunStore;
    private final JobDefinitionStore jobDefinitionStore;
    private final RunLeaseService runLeaseService;
    private final RunFinalizationService runFinalizationService;
    private final JobDefinitionEnvResolver environmentResolver;
    private final JobDefinitionOptionsFileWriter optionsFileWriter;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;
    private final ActiveRunRegistry activeRunRegistry;

    @Autowired
    public JobExecutionService(JobRunStore jobRunStore,
                               JobDefinitionStore jobDefinitionStore,
                               RunLeaseService runLeaseService,
                               RunFinalizationService runFinalizationService,
                               JobDefinitionEnvResolver environmentResolver,
                               JobDefinitionOptionsFileWriter optionsFileWriter,
                               AuditService auditService,
                               AuditActorResolver auditActorResolver,
                               ActiveRunRegistry activeRunRegistry) {
        this.jobRunStore = jobRunStore;
        this.jobDefinitionStore = jobDefinitionStore;
        this.runLeaseService = runLeaseService;
        this.runFinalizationService = runFinalizationService;
        this.environmentResolver = environmentResolver;
        this.optionsFileWriter = optionsFileWriter;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
        this.activeRunRegistry = activeRunRegistry;
    }

    public Optional<JobRunOutcome> executeNextPending(String executorIdentity) {
        Optional<JobRun> claimed = runLeaseService.claimNextEligible(executorIdentity,
                java.time.Duration.ofMinutes(5));
        return claimed.map(run -> executeClaimedRun(run, handle -> { }));
    }

    public JobRunOutcome executeClaimedRun(JobRun run, Consumer<RunExecutionHandle> onStarted) {
        ToolOptions options = null;
        RunExecutionHandle handle = null;
        Path optionsFile = null;
        try {
                JobDefinition definition = jobDefinitionStore.findById(run.jobDefinitionId())
                    .orElseThrow(() -> new IllegalStateException(
                            "JobDefinition not found for JobRun " + run.id()));
                String previousWatermark = jobRunStore.findLastCommittedWatermark(definition.id())
                    .orElse(definition.initialWatermarkValue());
                optionsFile = optionsFileWriter.write(definition, previousWatermark, environmentResolver::resolve);
                options = new ToolOptions(new String[]{"--options-file", optionsFile.toString()});
                handle = new RunExecutionHandle(run, options);
                if (!activeRunRegistry.register(handle)) {
                    throw new IllegalStateException("JobRun is already active locally: " + run.id());
                }
                onStarted.accept(handle);

            int exitCode = ReplicaDB.processReplica(options);
            JobRunStatus status = JobRunStatus.fromReplicaExitCode(exitCode);
            long rowsProcessed = options.getExecutionContext().getRowsProcessed();
            long durationMillis = options.getExecutionContext().getDurationMillis();
            LeaseToken leaseToken = requireLeaseToken(run);
            if (status == JobRunStatus.SUCCEEDED) {
                JobRunStore.FencedUpdateResult result = runFinalizationService.markSucceeded(run.id(), leaseToken,
                        rowsProcessed, durationMillis, options.getExecutionContext().getWatermarkCandidate());
                recordIfUpdated(result, run, status, rowsProcessed, durationMillis, null);
            } else if (status == JobRunStatus.CANCELLED) {
                JobRunStore.FencedUpdateResult result = runFinalizationService.markCancelled(
                        run.id(), leaseToken, rowsProcessed, durationMillis);
                recordIfUpdated(result, run, status, rowsProcessed, durationMillis, null);
            } else {
                String errorMessage = redactedFailureMessage("ReplicaDB execution failed for run " + run.id());
                JobRunStore.FencedUpdateResult result = runFinalizationService.markFailed(
                        run.id(), leaseToken, rowsProcessed, durationMillis, errorMessage);
                recordIfUpdated(result, run, JobRunStatus.FAILED, rowsProcessed, durationMillis, errorMessage);
            }
            return new JobRunOutcome(run.id(), status, rowsProcessed, durationMillis);
        } catch (Exception exception) {
            long rowsProcessed = options == null ? 0 : options.getExecutionContext().getRowsProcessed();
            long durationMillis = options == null ? 0 : options.getExecutionContext().getDurationMillis();
            String message = CredentialRedactor.redactMessage(exception.getMessage());
            if (message == null || message.isBlank()) {
                message = exception.getClass().getSimpleName();
            }
                JobRunStore.FencedUpdateResult result = runFinalizationService.markFailed(
                    run.id(), requireLeaseToken(run), rowsProcessed, durationMillis, message);
                recordIfUpdated(result, run, JobRunStatus.FAILED, rowsProcessed, durationMillis, message);
            return new JobRunOutcome(run.id(), JobRunStatus.FAILED, rowsProcessed, durationMillis);
        } finally {
            deleteOptionsFile(optionsFile);
            if (handle != null) {
                activeRunRegistry.remove(run.id(), handle);
            }
        }
    }

    ActiveRunRegistry activeRunRegistry() {
        return activeRunRegistry;
    }

    private static void deleteOptionsFile(Path optionsFile) {
        if (optionsFile == null) {
            return;
        }
        try {
            Files.deleteIfExists(optionsFile);
        } catch (IOException exception) {
            LOG.error("Could not delete temporary job options file", exception);
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

    private void recordIfUpdated(JobRunStore.FencedUpdateResult result, JobRun run, JobRunStatus status,
                                 long rowsProcessed, long durationMillis, String errorMessage) {
        if (result == JobRunStore.FencedUpdateResult.UPDATED) {
            recordTerminalOutcome(run, status, rowsProcessed, durationMillis, errorMessage);
            return;
        }
        LOG.warn("Ignoring stale finalization for JobRun {} with result {}", run.id(), result);
    }

    private static LeaseToken requireLeaseToken(JobRun run) {
        if (run.leaseToken() == null) {
            throw new IllegalStateException("JobRun " + run.id() + " has no lease token");
        }
        return run.leaseToken();
    }

    private static String redactedFailureMessage(String message) {
        String redacted = CredentialRedactor.redactMessage(message);
        if (redacted == null || redacted.isBlank()) {
            return "Job execution failed";
        }
        return redacted;
    }
}
