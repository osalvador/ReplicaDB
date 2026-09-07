package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.security.JobAccessService;
import org.springframework.context.annotation.Profile;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1/dashboard")
@Tag(name = "Dashboard", description = "Permission-aware run and throughput summaries.")
@SecurityRequirement(name = "sessionCookie")
public class DashboardController {

    private final JobRunStore jobRunStore;
    private final JobDefinitionStore jobDefinitionStore;
    private final JobAccessService jobAccessService;

    public DashboardController(JobRunStore jobRunStore, JobDefinitionStore jobDefinitionStore,
                               JobAccessService jobAccessService) {
        this.jobRunStore = jobRunStore;
        this.jobDefinitionStore = jobDefinitionStore;
        this.jobAccessService = jobAccessService;
    }

    @GetMapping("/summary")
        @Operation(operationId = "getDashboardSummary", summary = "Get the dashboard summary",
            description = "Aggregates only jobs visible to the authenticated identity. Omitted bounds produce an effective 24-hour window ending at the current server time; from must be before to.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Permission-aware dashboard summary returned"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem")
        })
    public DashboardSummaryResponse summary(
            @Parameter(description = "Optional inclusive window start in UTC ISO-8601 date-time format. Defaults to 24 hours before the effective end.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "Optional exclusive window end in UTC ISO-8601 date-time format. Defaults to current server time.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            Authentication authentication) {
        Instant effectiveTo = to == null ? Instant.now() : to;
        Instant effectiveFrom = from == null ? effectiveTo.minus(Duration.ofHours(24)) : from;
        if (!effectiveFrom.isBefore(effectiveTo)) {
            throw new IllegalArgumentException("Dashboard range must have a start before its end");
        }
        Optional<Set<UUID>> visibleJobIds = jobAccessService.visibleJobIds(authentication);
        Set<UUID> restriction = visibleJobIds.orElse(null);
        JobRunStore.DashboardRunSummary summary = jobRunStore.summarizeDashboard(
                effectiveFrom, effectiveTo, restriction);
        return DashboardSummaryResponse.from(effectiveFrom, effectiveTo,
                jobDefinitionStore.count(restriction), summary);
    }
}
