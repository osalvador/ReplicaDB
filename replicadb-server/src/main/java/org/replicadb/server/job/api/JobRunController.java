package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.application.RunCancellationService;
import org.replicadb.server.job.application.RunDispatchResult;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.execution.RunExecutionCoordinator;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunLogStore;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1")
@Tag(name = "Runs", description = "Dispatch, inspect, cancel, retry, and diagnose durable job runs.")
@SecurityRequirement(name = "sessionCookie")
public class JobRunController {

    private final JobRunStore jobRunStore;
    private final Optional<RunLogStore> runLogStore;
    private final JobDefinitionStore jobDefinitionStore;
    private final RunCancellationService runCancellationService;
    private final RunDispatchService runDispatchService;
    private final RunExecutionCoordinator executionCoordinator;
    private final JobAccessService jobAccessService;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;
    private final boolean localSeedingEnabled;
    private final boolean localExecutionEnabled;

    public JobRunController(JobRunStore jobRunStore,
                            Optional<RunLogStore> runLogStore,
                            JobDefinitionStore jobDefinitionStore,
                            RunCancellationService runCancellationService,
                            RunDispatchService runDispatchService,
                            RunExecutionCoordinator executionCoordinator,
                            JobAccessService jobAccessService,
                            AuditService auditService,
                            AuditActorResolver auditActorResolver,
                            @Value("${replicadb.server.local-seeding.enabled:false}") boolean localSeedingEnabled,
                            @Value("${replicadb.server.local-execution.enabled:true}") boolean localExecutionEnabled) {
        this.jobRunStore = jobRunStore;
        this.runLogStore = runLogStore;
        this.jobDefinitionStore = jobDefinitionStore;
        this.runCancellationService = runCancellationService;
        this.runDispatchService = runDispatchService;
        this.executionCoordinator = executionCoordinator;
        this.jobAccessService = jobAccessService;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
        this.localSeedingEnabled = localSeedingEnabled;
        this.localExecutionEnabled = localExecutionEnabled;
    }

    @GetMapping("/jobs/{jobDefinitionId}/runs")
        @Operation(operationId = "listJobRunsForJob", summary = "List runs for a job",
            description = "Returns run history for one job after VIEW permission. Status values are case-insensitive; paging defaults to page 0 and size 50 with a size cap of 200.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Job run history returned"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
    public PageResponse<JobRunResponse> listForJob(
            @Parameter(description = "Job definition identifier.", required = true) @PathVariable UUID jobDefinitionId,
            @Parameter(description = "Optional repeated run statuses: PENDING, RUNNING, SUCCEEDED, FAILED, CANCEL_REQUESTED, CANCELLED, or RETRY_SCHEDULED.")
            @RequestParam(required = false) List<String> status,
            @Parameter(description = "Inclusive lower timestamp bound in UTC ISO-8601 date-time format.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "Exclusive upper timestamp bound in UTC ISO-8601 date-time format.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @Parameter(description = "Zero-based page number.", schema = @Schema(defaultValue = "0", minimum = "0"))
            @RequestParam(required = false) Integer page,
            @Parameter(description = "Requested page size, clamped to the range 1 through 200.", schema = @Schema(defaultValue = "50", minimum = "1", maximum = "200"))
            @RequestParam(required = false) Integer size,
            Authentication authentication) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.VIEW);
        PageRequestParams params = PageRequestParams.of(page, size);
        Set<JobRunStatus> parsedStatuses = parseStatuses(status);
        return pageResponse(jobRunStore.findPage(jobDefinitionId, parsedStatuses, from, to,
                params.page(), params.size(), null),
            params, jobRunStore.count(jobDefinitionId, parsedStatuses, from, to, null));
    }

    @GetMapping("/runs")
        @Operation(operationId = "listJobRuns", summary = "List visible runs",
            description = "Returns runs only for jobs visible to the authenticated identity, with optional status and UTC time bounds.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Visible runs returned"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem")
        })
    public PageResponse<JobRunResponse> list(
            @Parameter(description = "Optional repeated run statuses: PENDING, RUNNING, SUCCEEDED, FAILED, CANCEL_REQUESTED, CANCELLED, or RETRY_SCHEDULED.")
            @RequestParam(required = false) List<String> status,
            @Parameter(description = "Inclusive lower timestamp bound in UTC ISO-8601 date-time format.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "Exclusive upper timestamp bound in UTC ISO-8601 date-time format.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @Parameter(description = "Zero-based page number.", schema = @Schema(defaultValue = "0", minimum = "0"))
            @RequestParam(required = false) Integer page,
            @Parameter(description = "Requested page size, clamped to the range 1 through 200.", schema = @Schema(defaultValue = "50", minimum = "1", maximum = "200"))
            @RequestParam(required = false) Integer size,
            Authentication authentication) {
        PageRequestParams params = PageRequestParams.of(page, size);
        Set<JobRunStatus> parsedStatuses = parseStatuses(status);
        Optional<Set<UUID>> visibleJobIds = jobAccessService.visibleJobIds(authentication);
        Set<UUID> restriction = visibleJobIds.orElse(null);
        return pageResponse(jobRunStore.findPage(null, parsedStatuses, from, to,
                params.page(), params.size(), restriction),
            params, jobRunStore.count(null, parsedStatuses, from, to, restriction));
    }

    @GetMapping("/runs/{id}")
        @Operation(operationId = "getJobRun", summary = "Get a run",
            description = "Returns durable state and attempt information after VIEW permission on the owning job.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Run returned"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public JobRunResponse get(
            @Parameter(description = "Run identifier.", required = true) @PathVariable UUID id,
            Authentication authentication) {
        JobRun run = findRun(id);
        jobAccessService.require(authentication, run.jobDefinitionId(), JobPermissionType.VIEW);
        return JobRunResponse.from(run);
    }

    @GetMapping("/runs/{id}/log")
        @Operation(operationId = "getJobRunLog", summary = "Get bounded run diagnostics",
            description = "Returns the credential-redacted, size-bounded log after VIEW permission. A run without captured output returns an empty log response.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Run diagnostics returned"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public RunLogResponse log(
            @Parameter(description = "Run identifier.", required = true) @PathVariable UUID id,
            Authentication authentication) {
        JobRun run = findRun(id);
        jobAccessService.require(authentication, run.jobDefinitionId(), JobPermissionType.VIEW);
        return runLogStore.flatMap(store -> store.findByRunId(run.id()))
            .map(RunLogResponse::from)
            .orElseGet(() -> RunLogResponse.empty(run.id()));
    }

    @PostMapping("/jobs/{jobDefinitionId}/runs")
        @Operation(operationId = "triggerJobRun", summary = "Trigger a manual run",
            description = "Creates or replays one pending manual run after EXECUTE permission. The idempotency key is scoped to manual trigger requests and remains replay-safe within the server retention window. Protected mutations require the session cookie and CSRF header.")
        @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Run accepted or an existing idempotent result replayed"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem"),
            @ApiResponse(responseCode = "409", ref = "#/components/responses/ConflictProblem")
        })
    public ResponseEntity<JobRunResponse> trigger(
            @Parameter(description = "Job definition identifier.", required = true) @PathVariable UUID jobDefinitionId,
            @Parameter(description = "Required non-blank key for replay-safe manual triggering; maximum 255 characters.", required = true,
                schema = @Schema(maxLength = 255, example = "orders-sync-20260907-001"))
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            Authentication authentication,
            @Parameter(hidden = true) HttpServletRequest request) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 255) {
            throw new IllegalArgumentException("Idempotency-Key must be present and at most 255 characters");
        }
        JobDefinition definition = findDefinition(jobDefinitionId);
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.EXECUTE);
        boolean localSeedRequested = "true".equalsIgnoreCase(request.getHeader("X-ReplicaDB-Local-Seed"));
        if (localSeedRequested && !localSeedingEnabled) {
            throw new IllegalStateException("Local run seeding is disabled");
        }
        if (localSeedRequested && !jobAccessService.isAdmin(authentication)) {
            throw new AccessDeniedException("Local run seeding requires ADMIN");
        }

        String warning = localSeedRequested ? cancellationWarning(definition.mode()) : null;
        RunDispatchResult dispatch = runDispatchService.dispatchManual(
                definition.id(), idempotencyKey, localSeedRequested, warning);
        JobRun pending = dispatch.run().orElseThrow(() -> new IllegalStateException(
                "Run dispatch did not return a JobRun"));
        if (dispatch.replayed()) {
            return accepted(pending);
        }
        if (localSeedRequested) {
            auditService.record(auditActorResolver.resolve(authentication), AuditAction.RUN_TRIGGERED,
                AuditResourceType.JOB_RUN, pending.id().toString(), AuditOutcome.SUCCESS,
                Map.of("jobDefinitionId", definition.id().toString(), "trigger", "local-seed"));
            auditService.record(auditActorResolver.resolve(authentication), AuditAction.RUN_CANCEL_REQUESTED,
                AuditResourceType.JOB_RUN, pending.id().toString(), AuditOutcome.SUCCESS,
                Map.of("warning", warning, "resultingStatus", pending.status().name()));
            return accepted(pending);
        }
        if (localExecutionEnabled) {
            executionCoordinator.submit(pending.id(), "api");
        }
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.RUN_TRIGGERED,
            AuditResourceType.JOB_RUN, pending.id().toString(), AuditOutcome.SUCCESS,
            Map.of("jobDefinitionId", definition.id().toString(), "trigger", "manual"));
        return accepted(pending);
    }

    @PostMapping("/runs/{id}/cancel")
        @Operation(operationId = "cancelJobRun", summary = "Request run cancellation",
            description = "Cancels pending work or persists cancellation intent for a running attempt after CANCEL permission. The response includes a mode-specific sink warning. Protected mutations require the session cookie and CSRF header.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Run cancelled or cancellation requested"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem"),
            @ApiResponse(responseCode = "409", ref = "#/components/responses/ConflictProblem")
        })
        public CancellationResponse cancel(
            @Parameter(description = "Run identifier.", required = true) @PathVariable UUID id,
            Authentication authentication) {
        JobRun run = findRun(id);
        jobAccessService.require(authentication, run.jobDefinitionId(), JobPermissionType.CANCEL);
        JobDefinition definition = findDefinition(run.jobDefinitionId());
        String warning = cancellationWarning(definition.mode());

        if (run.status() == JobRunStatus.PENDING) {
            runCancellationService.cancelPending(id, warning);
            return auditedCancellation(id, authentication, warning, JobRunStatus.CANCELLED);
        }
        if (run.status() != JobRunStatus.RUNNING) {
            throw new IllegalStateException("JobRun is not cancellable: " + id);
        }
        JobRunStore.CancellationResult cancellationResult = runCancellationService.requestCancellation(
                id, warning, executionCoordinator::requestCancellation);
        if (cancellationResult == JobRunStore.CancellationResult.NOT_FOUND
                || cancellationResult == JobRunStore.CancellationResult.TERMINAL) {
            throw new IllegalStateException("JobRun is no longer running: " + id);
        }
        JobRun current = findRun(id);
        if (current.status() == JobRunStatus.CANCELLED) {
            return auditedCancellation(id, authentication, warning, JobRunStatus.CANCELLED);
        }
        return auditedCancellation(id, authentication, warning, JobRunStatus.CANCEL_REQUESTED);
    }

    @PostMapping("/runs/{id}/retry")
        @Operation(operationId = "retryJobRun", summary = "Retry a failed run",
            description = "Creates a new pending attempt linked to the failed run after EXECUTE permission. Retry restarts from the beginning and is not resume behavior. Protected mutations require the session cookie and CSRF header.")
        @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Retry attempt accepted"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem"),
            @ApiResponse(responseCode = "409", ref = "#/components/responses/ConflictProblem")
        })
        public ResponseEntity<JobRunResponse> retry(
            @Parameter(description = "Failed run identifier.", required = true) @PathVariable UUID id,
            Authentication authentication) {
        JobRun failedRun = findRun(id);
        jobAccessService.require(authentication, failedRun.jobDefinitionId(), JobPermissionType.EXECUTE);
        if (failedRun.status() != JobRunStatus.FAILED) {
            throw new IllegalStateException("Only failed JobRuns can be retried: " + id);
        }
        RunDispatchResult dispatch = runDispatchService.dispatchRetry(id);
        JobRun retry = dispatch.run().orElseThrow(() -> new IllegalStateException(
            "Retry dispatch did not return a JobRun"));
        if (dispatch.created() && localExecutionEnabled) {
            executionCoordinator.submit(retry.id(), "api");
        }
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.RUN_RETRIED,
            AuditResourceType.JOB_RUN, retry.id().toString(), AuditOutcome.SUCCESS,
            Map.of("previousRunId", id.toString()));
        return accepted(retry);
    }

        private CancellationResponse auditedCancellation(UUID runId, Authentication authentication,
                                 String warning, JobRunStatus resultingStatus) {
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.RUN_CANCEL_REQUESTED,
            AuditResourceType.JOB_RUN, runId.toString(), AuditOutcome.SUCCESS,
            Map.of("warning", warning, "resultingStatus", resultingStatus.name()));
        return new CancellationResponse(runId, resultingStatus, warning);
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
        return jobRunStore.findById(id)
                .orElseThrow(() -> new NoSuchElementException("JobRun not found: " + id));
    }

            private JobDefinition findDefinition(UUID id) {
            return jobDefinitionStore.findById(id)
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

    private static Set<JobRunStatus> parseStatuses(List<String> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            return null;
        }

        Set<JobRunStatus> parsed = new java.util.LinkedHashSet<>();
        for (String status : statuses) {
            try {
                parsed.add(JobRunStatus.valueOf(status.toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("Unknown run status: " + status, exception);
            }
        }
        return parsed;
    }

    @Schema(description = "Durable result of a run cancellation request.")
    public record CancellationResponse(
            @Schema(description = "Run identifier.", format = "uuid") UUID runId,
            @Schema(description = "Resulting durable cancellation state.", allowableValues = {"CANCEL_REQUESTED", "CANCELLED"}) JobRunStatus status,
            @Schema(description = "Mode-specific warning about the possible sink state.") String warning) {
    }
}
