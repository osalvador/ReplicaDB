package org.replicadb.server.audit.api;

import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventFilter;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.replicadb.server.job.api.PageRequestParams;
import org.replicadb.server.job.api.PageResponse;
import org.springframework.format.annotation.DateTimeFormat;
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
@RequestMapping("/api/v1/audit")
@PreAuthorize("hasRole('ADMIN')")
public class AuditEventController {

    private final AuditEventRepository repository;

    public AuditEventController(AuditEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public PageResponse<AuditEventResponse> list(
            @RequestParam(required = false) UUID actorUserId,
            @RequestParam(required = false) String action,
            @RequestParam(required = false) String resourceType,
            @RequestParam(required = false) String resourceId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) Integer page,
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
