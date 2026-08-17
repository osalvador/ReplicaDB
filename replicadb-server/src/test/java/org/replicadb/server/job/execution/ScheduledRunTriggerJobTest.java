package org.replicadb.server.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.persistence.JobRunRepository;

import java.util.UUID;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScheduledRunTriggerJobTest {

    private JobRunRepository jobRunRepository;
    private RunExecutionCoordinator runExecutionCoordinator;
    private AuditService auditService;
    private AuditActorResolver auditActorResolver;
    private ScheduledRunTriggerJob job;

    @BeforeEach
    void setUp() {
        jobRunRepository = Mockito.mock(JobRunRepository.class);
        runExecutionCoordinator = Mockito.mock(RunExecutionCoordinator.class);
        auditService = Mockito.mock(AuditService.class);
        auditActorResolver = Mockito.mock(AuditActorResolver.class);
        job = new ScheduledRunTriggerJob();
        org.springframework.test.util.ReflectionTestUtils.setField(job, "jobRunRepository", jobRunRepository);
        org.springframework.test.util.ReflectionTestUtils.setField(
                job, "runExecutionCoordinator", runExecutionCoordinator);
            org.springframework.test.util.ReflectionTestUtils.setField(job, "auditService", auditService);
            org.springframework.test.util.ReflectionTestUtils.setField(job, "auditActorResolver", auditActorResolver);
    }

    @Test
    void skipsWhenAJobAlreadyHasAnActiveRun() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        when(jobRunRepository.hasActiveRun(jobDefinitionId)).thenReturn(true);

        job.execute(context(jobDefinitionId));

        verify(jobRunRepository, never()).insertPending(jobDefinitionId, null, 1);
        verify(runExecutionCoordinator, never()).submit(Mockito.any(), Mockito.anyString());
        verify(auditService, never()).record(Mockito.any(), Mockito.any(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    void insertsAndSubmitsWhenNoRunIsActive() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        JobRun pending = pendingRun(jobDefinitionId);
        when(jobRunRepository.hasActiveRun(jobDefinitionId)).thenReturn(false);
        when(jobRunRepository.insertPending(jobDefinitionId, null, 1)).thenReturn(pending);
        AuditActor actor = AuditActor.system("scheduler");
        when(auditActorResolver.system("scheduler")).thenReturn(actor);

        job.execute(context(jobDefinitionId));

        verify(jobRunRepository).insertPending(jobDefinitionId, null, 1);
        verify(runExecutionCoordinator).submit(pending.id(), "scheduler");
        verify(auditService).record(actor, AuditAction.RUN_TRIGGERED, AuditResourceType.JOB_RUN,
            pending.id().toString(), AuditOutcome.SUCCESS,
            Map.of("jobDefinitionId", jobDefinitionId.toString(), "trigger", "schedule"));
    }

    @Test
    void ignoresAConcurrentInsertRace() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        when(jobRunRepository.hasActiveRun(jobDefinitionId)).thenReturn(false);
        when(jobRunRepository.insertPending(jobDefinitionId, null, 1))
                .thenThrow(new IllegalStateException("active run"));

        assertDoesNotThrow(() -> job.execute(context(jobDefinitionId)));

        verify(runExecutionCoordinator, never()).submit(Mockito.any(), Mockito.anyString());
        verify(auditService, never()).record(Mockito.any(), Mockito.any(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.any());
    }

    private static JobExecutionContext context(UUID jobDefinitionId) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put("jobDefinitionId", jobDefinitionId.toString());
        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);
        when(context.getMergedJobDataMap()).thenReturn(dataMap);
        return context;
    }

    private static JobRun pendingRun(UUID jobDefinitionId) {
        return new JobRun(UUID.randomUUID(), jobDefinitionId, null, JobRunStatus.PENDING, 1,
            null, null, null, null, null, null, null, null, null, null, null);
    }
}
