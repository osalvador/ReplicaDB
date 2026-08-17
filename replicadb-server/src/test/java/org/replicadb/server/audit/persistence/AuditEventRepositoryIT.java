package org.replicadb.server.audit.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class AuditEventRepositoryIT {

    @Autowired
    private AuditEventRepository repository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE audit_event, app_user CASCADE", Map.of());
    }

    @Test
    void roundTripsDetailAndPersistenceFields() {
        Instant occurredAt = Instant.parse("2026-01-01T00:00:00Z");
        AuditEvent event = event(null, "admin", occurredAt, AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS,
                Map.of("name", "orders", "mode", "complete"));

        AuditEvent persisted = repository.insert(event);
        AuditEvent read = repository.findPage(AuditEventFilter.empty(), 0, 50).get(0);

        assertEquals(persisted, read);
        assertEquals(Map.of("name", "orders", "mode", "complete"), read.detail());
        assertEquals(persisted.id(), read.id());
        assertEquals(occurredAt, read.occurredAt());
    }

    @Test
    void acceptsNullActorUserAndResourceIds() {
        repository.insert(event(null, "unknown", Instant.now(), AuditAction.LOGIN_FAILED,
                AuditResourceType.SESSION, null, AuditOutcome.FAILURE, Map.of()));

        AuditEvent read = repository.findPage(AuditEventFilter.empty(), 0, 50).get(0);

        assertNull(read.actor().userId());
        assertEquals("unknown", read.actor().username());
        assertNull(read.resourceId());
    }

    @Test
    void filtersByEachSupportedFieldAndTimeWindow() {
        UUID firstActor = UUID.randomUUID();
        UUID secondActor = UUID.randomUUID();
        Instant base = Instant.parse("2026-02-01T00:00:00Z");
        appUserRepository.insert(new AppUser(firstActor, "first", "hash", GlobalRole.VIEWER,
                true, null, null));
        appUserRepository.insert(new AppUser(secondActor, "second", "hash", GlobalRole.VIEWER,
                true, null, null));
        repository.insert(event(firstActor, "first", base.plusSeconds(10), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS, Map.of()));
        repository.insert(event(secondActor, "second", base.plusSeconds(20), AuditAction.JOB_UPDATED,
                AuditResourceType.JOB_DEFINITION, "job-2", AuditOutcome.SUCCESS, Map.of()));
        repository.insert(event(firstActor, "first", base.plusSeconds(30), AuditAction.RUN_FAILED,
                AuditResourceType.JOB_RUN, "run-1", AuditOutcome.FAILURE, Map.of()));

        assertEquals(2, repository.findPage(new AuditEventFilter(firstActor, null, null, null,
                null, null), 0, 50).size());
        assertEquals(1, repository.findPage(new AuditEventFilter(null, AuditAction.JOB_UPDATED,
                null, null, null, null), 0, 50).size());
        assertEquals(1, repository.findPage(new AuditEventFilter(null, null, AuditResourceType.JOB_RUN,
                null, null, null), 0, 50).size());
        assertEquals(1, repository.findPage(new AuditEventFilter(null, null, null, "job-2",
                null, null), 0, 50).size());
        assertEquals(1, repository.findPage(new AuditEventFilter(null, null, null, null,
                base.plusSeconds(15), base.plusSeconds(25)), 0, 50).size());
    }

    @Test
    void paginatesNewestFirstAndCountsMatchingRows() {
        Instant base = Instant.parse("2026-03-01T00:00:00Z");
        AuditEvent older = repository.insert(event(null, "system:api", base,
                AuditAction.RUN_TRIGGERED, AuditResourceType.JOB_RUN, "run-old",
                AuditOutcome.SUCCESS, Map.of()));
        AuditEvent newer = repository.insert(event(null, "system:api", base.plusSeconds(1),
                AuditAction.RUN_TRIGGERED, AuditResourceType.JOB_RUN, "run-new",
                AuditOutcome.SUCCESS, Map.of()));

        List<AuditEvent> firstPage = repository.findPage(AuditEventFilter.empty(), 0, 1);
        List<AuditEvent> secondPage = repository.findPage(AuditEventFilter.empty(), 1, 1);

        assertEquals(List.of(newer), firstPage);
        assertEquals(List.of(older), secondPage);
        assertEquals(2, repository.count(AuditEventFilter.empty()));
    }

    @Test
    void validatesFilterBoundsAndPaging() {
        assertThrows(IllegalArgumentException.class, () -> new AuditEventFilter(null, null, null,
                null, Instant.parse("2026-04-02T00:00:00Z"), Instant.parse("2026-04-01T00:00:00Z")));
        assertThrows(IllegalArgumentException.class,
                () -> repository.findPage(AuditEventFilter.empty(), -1, 50));
        assertThrows(IllegalArgumentException.class,
                () -> repository.findPage(AuditEventFilter.empty(), 0, 0));
    }

    @Test
    void deletesOnlyEventsOlderThanRetentionWindow() {
        repository.insert(event(null, "system:api", Instant.now().minus(400, ChronoUnit.DAYS),
                AuditAction.RUN_FAILED, AuditResourceType.JOB_RUN, "old-run",
                AuditOutcome.FAILURE, Map.of()));
        AuditEvent recent = repository.insert(event(null, "system:api", Instant.now().minus(10,
                ChronoUnit.DAYS), AuditAction.RUN_SUCCEEDED, AuditResourceType.JOB_RUN, "recent-run",
                AuditOutcome.SUCCESS, Map.of()));

        assertEquals(1, repository.deleteOlderThan(365));
        assertEquals(List.of(recent), repository.findPage(AuditEventFilter.empty(), 0, 50));
    }

    @Test
    void retainsActorUsernameWhenReferencedUserIsDeleted() {
        AppUser user = appUserRepository.insert(new AppUser(null, "deleted-user", "hash",
                GlobalRole.VIEWER, true, null, null));
        repository.insert(event(user.id(), user.username(), Instant.now(), AuditAction.JOB_CREATED,
                AuditResourceType.JOB_DEFINITION, "job-1", AuditOutcome.SUCCESS, Map.of()));

        jdbcTemplate.update("DELETE FROM app_user WHERE id = :id", Map.of("id", user.id()));

        AuditEvent read = repository.findPage(AuditEventFilter.empty(), 0, 50).get(0);
        assertNull(read.actor().userId());
        assertEquals("deleted-user", read.actor().username());
    }

    private static AuditEvent event(UUID actorUserId, String actorUsername, Instant occurredAt,
                                    AuditAction action, AuditResourceType resourceType,
                                    String resourceId, AuditOutcome outcome,
                                    Map<String, String> detail) {
        return new AuditEvent(UUID.randomUUID(), occurredAt,
                new AuditActor(actorUserId, actorUsername, "127.0.0.1"), action, resourceType,
                resourceId, outcome, detail);
    }
}
