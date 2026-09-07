package org.replicadb.server.audit.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.job.api.PageRequestParams;
import org.replicadb.server.job.api.PageResponse;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.context.annotation.Profile;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1/audit")
@PreAuthorize("hasRole('ADMIN')")
@Tag(name = "Audit", description = "ADMIN-filtered, paginated durable audit history.")
@SecurityRequirement(name = "sessionCookie")
public class AuditEventController {

    private final AuditEventRepository repository;

    public AuditEventController(AuditEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping
        @Operation(operationId = "listAuditEvents", summary = "List audit events",
            description = "Returns durable audit events matching optional actor, action, resource, and UTC time filters. ADMIN is required.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Audit events returned"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem")
        })
    public PageResponse<AuditEventResponse> list(
            @Parameter(description = "Actor user identifier. System actors do not have a user identifier.")
            @RequestParam(required = false) UUID actorUserId,
            @Parameter(description = "Case-insensitive AuditAction enum name.")
            @RequestParam(required = false) String action,
            @Parameter(description = "Case-insensitive resource category.", schema = @Schema(allowableValues = {"USER", "DATASOURCE", "JOB_DEFINITION", "JOB_RUN", "SESSION"}))
            @RequestParam(required = false) String resourceType,
            @Parameter(description = "Exact audited resource identifier.")
            @RequestParam(required = false) String resourceId,
            @Parameter(description = "Inclusive lower event timestamp bound in UTC ISO-8601 date-time format.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "Exclusive upper event timestamp bound in UTC ISO-8601 date-time format.", schema = @Schema(format = "date-time"))
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @Parameter(description = "Zero-based page number.", schema = @Schema(defaultValue = "0", minimum = "0"))
            @RequestParam(required = false) Integer page,
            @Parameter(description = "Requested page size, clamped to the range 1 through 200.", schema = @Schema(defaultValue = "50", minimum = "1", maximum = "200"))
            @RequestParam(required = false) Integer size) {
        AuditEventFilter filter = new AuditEventFilter(actorUserId, parseAction(action),
                parseResourceType(resourceType), resourceId, from, to);
        PageRequestParams params = PageRequestParams.of(page, size);
        List<AuditEventResponse> events = repository.findPage(filter, params.page(), params.size()).stream()
                .map(AuditEventResponse::from)
                .toList();
        return new PageResponse<>(events, params.page(), params.size(), repository.count(filter));
    }

    private static AuditAction parseAction(String value) {
        if (value == null) {
            return null;
        }
        try {
            return AuditAction.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unknown audit action: " + value, exception);
        }
    }

    private static AuditResourceType parseResourceType(String value) {
        if (value == null) {
            return null;
        }
        try {
            return AuditResourceType.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unknown audit resource type: " + value, exception);
        }
    }
}
