package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.JobSchedule;
import org.replicadb.server.job.execution.QuartzScheduleService;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobScheduleRepository;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.JobPermissionType;
import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.NoSuchElementException;
import java.util.Map;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1/jobs/{jobDefinitionId}/schedule")
@Validated
@Tag(name = "Schedules", description = "Read and manage one recurring Quartz schedule per job.")
@SecurityRequirement(name = "sessionCookie")
public class JobScheduleController {

    private final JobDefinitionRepository jobDefinitionRepository;
    private final JobScheduleRepository jobScheduleRepository;
    private final QuartzScheduleService quartzScheduleService;
    private final JobAccessService jobAccessService;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public JobScheduleController(JobDefinitionRepository jobDefinitionRepository,
                                 JobScheduleRepository jobScheduleRepository,
                                 QuartzScheduleService quartzScheduleService,
                                 JobAccessService jobAccessService,
                                 AuditService auditService,
                                 AuditActorResolver auditActorResolver) {
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.jobScheduleRepository = jobScheduleRepository;
        this.quartzScheduleService = quartzScheduleService;
        this.jobAccessService = jobAccessService;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @PutMapping
        @Operation(operationId = "upsertJobSchedule", summary = "Create or replace a job schedule",
            description = "Validates and stores the Quartz CRON schedule after EDIT permission, then reconciles the durable scheduler trigger. Blank time zones default to UTC. Protected mutations require the session cookie and CSRF header.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Schedule created or replaced"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public JobScheduleResponse upsert(
                          @Parameter(description = "Job definition identifier.", required = true) @PathVariable UUID jobDefinitionId,
                                      @Valid @RequestBody JobScheduleRequest request,
                                      Authentication authentication) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.EDIT);
        findDefinition(jobDefinitionId);
        String timeZone = request.timeZone() == null || request.timeZone().isBlank()
                ? "UTC" : request.timeZone();
        JobSchedule schedule = new JobSchedule(
                jobDefinitionId, request.cronExpression(), timeZone, request.enabled(), null, null);
        JobSchedule persisted = jobScheduleRepository.upsert(schedule);
        quartzScheduleService.schedule(persisted);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_SCHEDULE_UPSERTED,
            AuditResourceType.JOB_DEFINITION, jobDefinitionId.toString(), AuditOutcome.SUCCESS,
            Map.of("cronExpression", persisted.cronExpression(), "timeZone", persisted.timeZone(),
                "enabled", Boolean.toString(persisted.enabled())));
        return response(persisted);
    }

    @GetMapping
        @Operation(operationId = "getJobSchedule", summary = "Get a job schedule",
            description = "Returns the recurring schedule and next fire time after VIEW permission. A job without a schedule returns not found.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Schedule returned"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public JobScheduleResponse get(
            @Parameter(description = "Job definition identifier.", required = true) @PathVariable UUID jobDefinitionId,
            Authentication authentication) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.VIEW);
        JobSchedule schedule = jobScheduleRepository.findByJobDefinitionId(jobDefinitionId)
                .orElseThrow(() -> new NoSuchElementException(
                        "JobSchedule not found for job definition: " + jobDefinitionId));
        return response(schedule);
    }

    @DeleteMapping
        @Operation(operationId = "deleteJobSchedule", summary = "Delete a job schedule",
            description = "Removes the persisted schedule and Quartz trigger after EDIT permission. Repeated deletion is idempotent. Protected mutations require the session cookie and CSRF header.")
        @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Schedule removed or already absent"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public ResponseEntity<Void> delete(
            @Parameter(description = "Job definition identifier.", required = true) @PathVariable UUID jobDefinitionId,
            Authentication authentication) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.EDIT);
        jobScheduleRepository.delete(jobDefinitionId);
        quartzScheduleService.unschedule(jobDefinitionId);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_SCHEDULE_DELETED,
            AuditResourceType.JOB_DEFINITION, jobDefinitionId.toString(), AuditOutcome.SUCCESS);
        return ResponseEntity.noContent().build();
    }

    private void findDefinition(UUID jobDefinitionId) {
        jobDefinitionRepository.findById(jobDefinitionId)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + jobDefinitionId));
    }

    private JobScheduleResponse response(JobSchedule schedule) {
        return JobScheduleResponse.from(schedule,
                quartzScheduleService.nextFireTime(schedule.jobDefinitionId()).orElse(null));
    }
}
