package org.replicadb.server.job.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.application.RunFinalizationService;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.config.WorkerRuntimeProperties;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.observability.WorkerBusySlotTracker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class WorkerDispatchCoordinatorIT {

    private static final Duration LEASE_DURATION = Duration.ofSeconds(5);

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private RunLeaseService runLeaseService;

    @Autowired
    private RunFinalizationService runFinalizationService;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private WorkerDispatchCoordinator firstCoordinator;
    private WorkerDispatchCoordinator secondCoordinator;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition, datasource_permission, "
            + "managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @AfterEach
    void stopCoordinators() {
        if (firstCoordinator != null) {
            firstCoordinator.shutdown(Duration.ofSeconds(2));
        }
        if (secondCoordinator != null) {
            secondCoordinator.shutdown(Duration.ofSeconds(2));
        }
    }

    @Test
    void twoWorkersClaimDistinctRunsExactlyOnceDespiteDuplicateSignals() throws Exception {
        JobRun firstRun = pendingRun();
        JobRun secondRun = pendingRun();
        JobExecutionService executionService = mock(JobExecutionService.class);
        ActiveRunRegistry firstRegistry = new ActiveRunRegistry();
        ActiveRunRegistry secondRegistry = new ActiveRunRegistry();
        firstCoordinator = coordinator("worker-one", executionService, firstRegistry);
        secondCoordinator = coordinator("worker-two", executionService, secondRegistry);
        CountDownLatch firstExecutionStarted = new CountDownLatch(1);
        CountDownLatch executionsStarted = new CountDownLatch(2);
        CountDownLatch releaseExecutions = new CountDownLatch(1);
        AtomicInteger executionCount = new AtomicInteger();
        doAnswer(invocation -> {
            ClaimedRunPreparation preparation = invocation.getArgument(0);
            JobRun run = preparation.run();
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            executionCount.incrementAndGet();
            firstExecutionStarted.countDown();
            executionsStarted.countDown();
            releaseExecutions.await(5, TimeUnit.SECONDS);
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(executionService).executeClaimedRun(any(), any());

        firstCoordinator.signalRun(firstRun.id());
        secondCoordinator.signalRun(firstRun.id());
        assertTrue(firstExecutionStarted.await(5, TimeUnit.SECONDS));
        firstCoordinator.signalRun(secondRun.id());
        secondCoordinator.signalRun(secondRun.id());
        firstCoordinator.signalRun(secondRun.id());
        secondCoordinator.signalRun(secondRun.id());

        assertTrue(executionsStarted.await(5, TimeUnit.SECONDS));
        List<JobRun> claimedRuns = List.of(
                jobRunRepository.findById(firstRun.id()).orElseThrow(),
                jobRunRepository.findById(secondRun.id()).orElseThrow());
        assertEquals(2, executionCount.get());
        assertTrue(claimedRuns.stream().allMatch(run -> run.status() == JobRunStatus.RUNNING));
        assertEquals(2, new HashSet<>(claimedRuns.stream()
                .map(JobRun::executorIdentity).toList()).size());
        releaseExecutions.countDown();
    }

    @Test
    void oneWorkerContinuesClaimingAfterTheOtherStops() throws Exception {
        JobRun run = pendingRun();
        JobExecutionService executionService = mock(JobExecutionService.class);
        firstCoordinator = coordinator("stopped-worker", executionService, new ActiveRunRegistry());
        secondCoordinator = coordinator("continuing-worker", executionService, new ActiveRunRegistry());
        CountDownLatch executed = new CountDownLatch(1);
        doAnswer(invocation -> {
            ClaimedRunPreparation preparation = invocation.getArgument(0);
            RunExecutionHandle handle = new RunExecutionHandle(preparation.run(), options());
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            executed.countDown();
            return new JobRunOutcome(handle.runId(), JobRunStatus.SUCCEEDED, 0, 0);
        }).when(executionService).executeClaimedRun(any(), any());

        firstCoordinator.stopAccepting();
        firstCoordinator.signalRun(run.id());
        secondCoordinator.signalEligibleWork();

        assertTrue(executed.await(5, TimeUnit.SECONDS));
        assertEquals("continuing-worker", jobRunRepository.findById(run.id()).orElseThrow().executorIdentity());
    }

    @Test
    void staleWorkerFinalizationIsFencedAfterReplacementClaim() {
        JobDefinition definition = jobDefinitionWithRetry();
        JobRun claimed = jobRunRepository.claimNextEligible(
                jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
                "expired-worker", LEASE_DURATION).orElseThrow();
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
                Map.of("id", claimed.id()));
        JobRun replacement = jobRunRepository.recoverExpiredRun(claimed.id()).replacementRun().orElseThrow();

        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
                runFinalizationService.markSucceeded(claimed.id(), claimed.leaseToken(), 99, 99, "stale"));
        assertTrue(jobRunRepository.findLastCommittedWatermark(definition.id()).isEmpty());
        assertEquals(JobRunStatus.PENDING, jobRunRepository.findById(replacement.id()).orElseThrow().status());
    }

    private WorkerDispatchCoordinator coordinator(String identity,
                                                  JobExecutionService executionService,
                                                  ActiveRunRegistry registry) {
        HeartbeatService heartbeatService = mock(HeartbeatService.class);
        when(heartbeatService.start(any())).thenAnswer(invocation -> new HeartbeatHandle(() -> { }));
        return new WorkerDispatchCoordinator(
                runLeaseService, jobRunRepository, executionService, registry, heartbeatService,
                new WorkerRunIdentity(identity), 1, LEASE_DURATION, Duration.ofSeconds(2),
                metrics(), policy(), new WorkerAdmissionScheduler(),
                tracker(identity, 1), 1_024);
    }

    private static ManagedRuntimeMetrics metrics() {
        return new ManagedRuntimeMetrics(new SimpleMeterRegistry());
    }

    private static WorkerBusySlotTracker tracker(String identity, int capacity) {
        return new WorkerBusySlotTracker(new SimpleMeterRegistry(), identity, capacity, System::nanoTime);
    }

    private static WorkerAdmissionPolicy policy() {
        WorkerRuntimeProperties.Admission admission = new WorkerRuntimeProperties.Admission();
        admission.setJitterMax(Duration.ZERO);
        admission.setGenericCooldown(Duration.ZERO);
        admission.getAdaptiveBackoff().setEnabled(false);
        return new WorkerAdmissionPolicy(admission, System::nanoTime, () -> 0.0);
    }

    private JobRun pendingRun() {
        JobDefinition definition = jobDefinitionRepository.insert(
                JobDefinitionTestFixtures.aJobDefinition()
                        .withName("worker-coordinator-" + UUID.randomUUID())
                    .withDefaultDatasourceReferences()
                        .build());
        return jobRunRepository.insertPendingNow(definition.id(), null, 1);
    }

    private JobDefinition jobDefinitionWithRetry() {
        return jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName("worker-recovery-" + UUID.randomUUID())
            .withDefaultDatasourceReferences()
                .withRetryPolicy(new RetryPolicy(3, 0, true))
                .build());
    }

    private static ToolOptions options() {
        try {
            return new ToolOptions(new String[]{
                    "--source-connect", "jdbc:sqlite:source.db",
                    "--sink-connect", "jdbc:sqlite:sink.db"
            });
        } catch (Exception exception) {
            throw new IllegalStateException("Could not create test options", exception);
        }
    }
}
