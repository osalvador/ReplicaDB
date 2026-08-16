package org.replicadb.server.job.execution;

import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class JobExecutionService {

    private final JobRunRepository jobRunRepository;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final JobDefinitionEnvResolver environmentResolver;
    private final ToolOptionsArgsBuilder argumentsBuilder;

    public JobExecutionService(JobRunRepository jobRunRepository,
                               JobDefinitionRepository jobDefinitionRepository,
                               JobDefinitionEnvResolver environmentResolver,
                               ToolOptionsArgsBuilder argumentsBuilder) {
        this.jobRunRepository = jobRunRepository;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.environmentResolver = environmentResolver;
        this.argumentsBuilder = argumentsBuilder;
    }

    public Optional<JobRunOutcome> executeNextPending(String executorIdentity) {
        Optional<JobRun> claimed = jobRunRepository.claimNextPending(executorIdentity,
                java.time.Duration.ofMinutes(5));
        return claimed.map(this::executeClaimedRun);
    }

    private JobRunOutcome executeClaimedRun(JobRun run) {
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

            int exitCode = ReplicaDB.processReplica(options);
            JobRunStatus status = JobRunStatus.fromReplicaExitCode(exitCode);
            long rowsProcessed = options.getExecutionContext().getRowsProcessed();
            long durationMillis = options.getExecutionContext().getDurationMillis();
            if (status == JobRunStatus.SUCCEEDED) {
                jobRunRepository.markSucceeded(run.id(), rowsProcessed, durationMillis,
                        options.getExecutionContext().getWatermarkCandidate());
            } else if (status == JobRunStatus.CANCELLED) {
                jobRunRepository.markCancelled(run.id(), rowsProcessed, durationMillis);
            } else {
                jobRunRepository.markFailed(run.id(), rowsProcessed, durationMillis,
                        "ReplicaDB execution failed for run " + run.id());
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
            return new JobRunOutcome(run.id(), JobRunStatus.FAILED, rowsProcessed, durationMillis);
        }
    }
}