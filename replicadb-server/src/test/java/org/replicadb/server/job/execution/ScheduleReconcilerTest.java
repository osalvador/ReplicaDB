package org.replicadb.server.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobSchedule;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobScheduleRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class ScheduleReconcilerTest {

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private ScheduleReconciler reconciler;

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_schedule, job_run, job_definition, datasource_permission, "
            + "managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @Test
    void registersEnabledRowsAndLeavesDisabledRowsUnscheduled() throws Exception {
        JobDefinition first = jobDefinition();
        JobDefinition second = jobDefinition();
        JobDefinition disabled = jobDefinition();
        first = jobDefinitionRepository.insert(first);
        second = jobDefinitionRepository.insert(second);
        disabled = jobDefinitionRepository.insert(disabled);
        insertSchedule(first.id(), true);
        insertSchedule(second.id(), true);
        insertSchedule(disabled.id(), false);

        reconciler.run(new DefaultApplicationArguments());

        assertTrue(scheduler.checkExists(triggerKey(first.id())));
        assertTrue(scheduler.checkExists(triggerKey(second.id())));
        assertFalse(scheduler.checkExists(triggerKey(disabled.id())));
    }

    @Test
    void continuesReconcilingWhenOneScheduleFails() {
        JobScheduleRepository repository = Mockito.mock(JobScheduleRepository.class);
        QuartzScheduleService service = Mockito.mock(QuartzScheduleService.class);
        ScheduleReconciler isolatedReconciler = new ScheduleReconciler(repository, service);
        JobSchedule failed = schedule(UUID.randomUUID());
        JobSchedule successful = schedule(UUID.randomUUID());
        when(repository.findAllEnabled()).thenReturn(List.of(failed, successful));
        doThrow(new IllegalStateException("registration failure")).when(service).schedule(failed);

        assertDoesNotThrow(() -> isolatedReconciler.run(new DefaultApplicationArguments()));

        verify(service).schedule(failed);
        verify(service).schedule(successful);
    }

    private void insertSchedule(UUID jobDefinitionId, boolean enabled) {
        jdbcTemplate.update("""
                INSERT INTO job_schedule (job_definition_id, cron_expression, time_zone, enabled)
                VALUES (:jobDefinitionId, :cronExpression, :timeZone, :enabled)
                """, Map.of(
                "jobDefinitionId", jobDefinitionId,
                "cronExpression", "0 0 1 1 1 ?",
                "timeZone", "UTC",
                "enabled", enabled));
    }

    private static JobDefinition jobDefinition() {
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName("reconciler-job-" + UUID.randomUUID())
            .withDefaultDatasourceReferences()
            .build();
    }

    private static JobSchedule schedule(UUID jobDefinitionId) {
        Instant now = Instant.now();
        return new JobSchedule(jobDefinitionId, "0 0 1 1 1 ?", "UTC", true, now, now);
    }

    private static TriggerKey triggerKey(UUID jobDefinitionId) {
        return new TriggerKey(jobDefinitionId.toString(), "replicadb-jobs");
    }
}
