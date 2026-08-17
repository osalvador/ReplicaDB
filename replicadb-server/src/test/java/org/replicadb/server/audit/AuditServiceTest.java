package org.replicadb.server.audit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.audit.persistence.AuditEventRepository;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

    @Mock
    private AuditEventRepository repository;

    @Test
    void persistsEventMetadata() {
        AuditService service = new AuditService(repository);

        service.record(AuditActor.system("api"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS,
                Map.of("name", "orders"));

        AuditEvent event = capturedEvent();
        assertNotNull(event.id());
        assertNotNull(event.occurredAt());
        assertEquals(AuditAction.JOB_CREATED, event.action());
        assertEquals(AuditResourceType.JOB_DEFINITION, event.resourceType());
        assertEquals("job-1", event.resourceId());
        assertEquals(AuditOutcome.SUCCESS, event.outcome());
    }

    @Test
    void redactsDetailValuesBeforePersistence() {
        AuditService service = new AuditService(repository);

        service.record(AuditActor.system("api"), AuditAction.LOGIN_FAILED,
                AuditResourceType.SESSION, "user", AuditOutcome.FAILURE,
                Map.of("message", "authentication failed password=secret"));

        AuditEvent event = capturedEvent();
        assertFalse(event.detail().get("message").contains("secret"));
    }

    @Test
    void truncatesLongDetailValues() {
        AuditService service = new AuditService(repository);

        service.record(AuditActor.system("api"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS,
                Map.of("value", "x".repeat(2000)));

        assertEquals(1000, capturedEvent().detail().get("value").length());
    }

    @Test
    void omitsNullAndBlankDetailValues() {
        AuditService service = new AuditService(repository);
        Map<String, String> detail = new HashMap<>();
        detail.put("nullValue", null);
        detail.put("blankValue", "  ");
        detail.put("present", "value");

        service.record(AuditActor.system("api"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS, detail);

        assertEquals(Map.of("present", "value"), capturedEvent().detail());
    }

    @Test
    void normalizesNullDetail() {
        AuditService service = new AuditService(repository);

        service.record(AuditActor.system("api"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS, null);

        assertEquals(Map.of(), capturedEvent().detail());
    }

    @Test
    void swallowsRepositoryFailure() {
        doThrow(new RuntimeException("database unavailable")).when(repository).insert(
                org.mockito.ArgumentMatchers.any(AuditEvent.class));
        AuditService service = new AuditService(repository);

        assertDoesNotThrow(() -> service.record(AuditActor.system("api"), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS));
    }

    @Test
    void noDetailOverloadPersistsEmptyDetail() {
        AuditService service = new AuditService(repository);

        service.record(AuditActor.system("api"), AuditAction.LOGOUT,
                AuditResourceType.SESSION, "admin", AuditOutcome.SUCCESS);

        assertEquals(Map.of(), capturedEvent().detail());
    }

    private AuditEvent capturedEvent() {
        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(repository).insert(captor.capture());
        return captor.getValue();
    }
}
