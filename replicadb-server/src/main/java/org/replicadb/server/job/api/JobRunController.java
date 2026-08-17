package org.replicadb.server.job.api;

import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.execution.RunExecutionCoordinator;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.RunTriggerIdempotencyRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;

import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class JobRunController {

    private final JobRunRepository jobRunRepository;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final RunTriggerIdempotencyRepository idempotencyRepository;
    private final RunExecutionCoordinator executionCoordinator;
    private final JobAccessService jobAccessService;

    public JobRunController(JobRunRepository jobRunRepository,
                            JobDefinitionRepository jobDefinitionRepository,
                            RunTriggerIdempotencyRepository idempotencyRepository,
                            RunExecutionCoordinator executionCoordinator,
                            JobAccessService jobAccessService) {
        this.jobRunRepository = jobRunRepository;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.idempotencyRepository = idempotencyRepository;
        this.executionCoordinator = executionCoordinator;
        this.jobAccessService = jobAccessService;
    }

    @GetMapping("/jobs/{jobDefinitionId}/runs")
    public PageResponse<JobRunResponse> listForJob(
            @PathVariable UUID jobDefinitionId,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            Authentication authentication) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.VIEW);
        PageRequestParams params = PageRequestParams.of(page, size);
        return pageResponse(jobRunRepository.findPage(jobDefinitionId, null, params.page(), params.size(), null),
            params, jobRunRepository.count(jobDefinitionId, null, null));
    }

    @GetMapping("/runs")
    public PageResponse<JobRunResponse> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            Authentication authentication) {
        PageRequestParams params = PageRequestParams.of(page, size);
        JobRunStatus parsedStatus = parseStatus(status);
        Optional<Set<UUID>> visibleJobIds = jobAccessService.visibleJobIds(authentication);
        Set<UUID> restriction = visibleJobIds.orElse(null);
        return pageResponse(jobRunRepository.findPage(null, parsedStatus, params.page(), params.size(), restriction),
            params, jobRunRepository.count(null, parsedStatus, restriction));
    }

    @GetMapping("/runs/{id}")
    public JobRunResponse get(@PathVariable UUID id, Authentication authentication) {
        JobRun run = findRun(id);
        jobAccessService.require(authentication, run.jobDefinitionId(), JobPermissionType.VIEW);
        return JobRunResponse.from(run);
    }

    @GetMapping("/runs/{id}/log")
    public RunLogResponse log(@PathVariable UUID id, Authentication authentication) {
        JobRun run = findRun(id);
        jobAccessService.require(authentication, run.jobDefinitionId(), JobPermissionType.VIEW);
        return new RunLogResponse(run.id(), run.errorMessage() == null ? "" : run.errorMessage());
    }

    @PostMapping("/jobs/{jobDefinitionId}/runs")
    public ResponseEntity<JobRunResponse> trigger(
            @PathVariable UUID jobDefinitionId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            Authentication authentication) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 255) {
            throw new IllegalArgumentException("Idempotency-Key must be present and at most 255 characters");
        }
        JobDefinition definition = findDefinition(jobDefinitionId);
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.EXECUTE);
        Optional<UUID> existingRunId = idempotencyRepository.findValidRunId(idempotencyKey);
        if (existingRunId.isPresent()) {
            JobRun existingRun = findRun(existingRunId.get());
            if (!jobDefinitionId.equals(existingRun.jobDefinitionId())) {
                throw new IllegalStateException("Idempotency-Key is already used for another job");
            }
            return accepted(existingRun);
        }
        if (jobRunRepository.hasActiveRun(jobDefinitionId)) {
            throw new IllegalStateException("Job definition " + jobDefinitionId + " already has an active run");
        }

        JobRun pending = jobRunRepository.insertPending(definition.id(), null, 1);
        idempotencyRepository.upsert(idempotencyKey, definition.id(), pending.id());
        executionCoordinator.submit(pending.id(), "api");
        return accepted(pending);
    }

    @PostMapping("/runs/{id}/cancel")
    public CancellationResponse cancel(@PathVariable UUID id, Authentication authentication) {
        JobRun run = findRun(id);
        jobAccessService.require(authentication, run.jobDefinitionId(), JobPermissionType.CANCEL);
        JobDefinition definition = findDefinition(run.jobDefinitionId());
        String warning = cancellationWarning(definition.mode());

        if (run.status() == JobRunStatus.PENDING) {
            jobRunRepository.markPendingCancelled(id);
            return new CancellationResponse(id, JobRunStatus.CANCELLED, warning);
        }
        if (run.status() != JobRunStatus.RUNNING) {
            throw new IllegalStateException("JobRun is not cancellable: " + id);
        }
        if (!executionCoordinator.requestCancellation(id)) {
            JobRun current = findRun(id);
            if (current.status().isTerminal()) {
                throw new IllegalStateException("JobRun is no longer running: " + id);
            }
            throw new IllegalStateException("JobRun is not registered for cancellation: " + id);
        }
        jobRunRepository.markCancelRequested(id);
        JobRun current = findRun(id);
        if (current.status() == JobRunStatus.CANCELLED) {
            return new CancellationResponse(id, JobRunStatus.CANCELLED, warning);
        }
        return new CancellationResponse(id, JobRunStatus.CANCEL_REQUESTED, warning);
    }

    @PostMapping("/runs/{id}/retry")
    public ResponseEntity<JobRunResponse> retry(@PathVariable UUID id, Authentication authentication) {
        JobRun failedRun = findRun(id);
        jobAccessService.require(authentication, failedRun.jobDefinitionId(), JobPermissionType.EXECUTE);
        if (failedRun.status() != JobRunStatus.FAILED) {
            throw new IllegalStateException("Only failed JobRuns can be retried: " + id);
        }
        JobRun retry = jobRunRepository.scheduleRetry(id);
        executionCoordinator.submit(retry.id(), "api");
        return accepted(retry);
    }

    private ResponseEntity<JobRunResponse> accepted(JobRun run) {
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/runs/" + run.id()))
                .body(JobRunResponse.from(run));
    }

    private PageResponse<JobRunResponse> pageResponse(java.util.List<JobRun> runs,
                                                       PageRequestParams params, long totalElements) {
        return new PageResponse<>(runs.stream().map(JobRunResponse::from).toList(),
                params.page(), params.size(), totalElements);
    }

    private JobRun findRun(UUID id) {
        return jobRunRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("JobRun not found: " + id));
    }

            private JobDefinition findDefinition(UUID id) {
            return jobDefinitionRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + id));
            }

            private static String cancellationWarning(ReplicationMode mode) {
            return switch (mode) {
                case INCREMENTAL ->
                    "Cancellation may leave partially merged rows; the watermark is not advanced.";
                case COMPLETE_ATOMIC ->
                    "Cancellation during the atomic swap may leave the sink in an indeterminate state.";
                case COMPLETE ->
                    "Cancellation may leave the sink truncated or partially loaded.";
            };
            }

    private static JobRunStatus parseStatus(String status) {
        if (status == null) {
            return null;
        }
        try {
            return JobRunStatus.valueOf(status.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unknown run status: " + status, exception);
        }
    }

    public record RunLogResponse(UUID runId, String excerpt) {
    }

    public record CancellationResponse(UUID runId, JobRunStatus status, String warning) {
    }
}
