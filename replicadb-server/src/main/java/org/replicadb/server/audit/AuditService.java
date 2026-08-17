package org.replicadb.server.audit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class AuditService {

    private static final Logger LOG = LogManager.getLogger(AuditService.class);
    private static final int MAX_DETAIL_VALUE_LENGTH = 1000;

    private final AuditEventRepository repository;

    public AuditService(AuditEventRepository repository) {
        this.repository = repository;
    }

    public void record(AuditActor actor, AuditAction action, AuditResourceType resourceType,
                       String resourceId, AuditOutcome outcome, Map<String, String> detail) {
        try {
            AuditEvent event = new AuditEvent(UUID.randomUUID(), Instant.now(), actor, action,
                    resourceType, resourceId, outcome, sanitizeDetail(detail));
            repository.insert(event);
        } catch (RuntimeException exception) {
            LOG.error("Failed to record audit event {} for {} {}", action, resourceType, resourceId,
                    exception);
        }
    }

    public void record(AuditActor actor, AuditAction action, AuditResourceType resourceType,
                       String resourceId, AuditOutcome outcome) {
        record(actor, action, resourceType, resourceId, outcome, Map.of());
    }

    private static Map<String, String> sanitizeDetail(Map<String, String> detail) {
        if (detail == null || detail.isEmpty()) {
            return Map.of();
        }

        Map<String, String> sanitized = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : detail.entrySet()) {
            String value = CredentialRedactor.redactMessage(entry.getValue());
            if (value == null || value.isBlank()) {
                continue;
            }
            sanitized.put(entry.getKey(), truncate(value));
        }
        return sanitized;
    }

    private static String truncate(String value) {
        return value.length() <= MAX_DETAIL_VALUE_LENGTH
                ? value
                : value.substring(0, MAX_DETAIL_VALUE_LENGTH);
    }
}
