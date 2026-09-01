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
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.observability.ManagedRuntimeMetrics;
import org.replicadb.server.observability.WorkerBusySlotTracker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class HybridWorkerDistributionIT {

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

    private final List<WorkerDispatchCoordinator> coordinators = new ArrayList<>();

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition, datasource_permission, "
            + "managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @AfterEach
    void stopCoordinators() {
        coordinators.forEach(coordinator -> coordinator.shutdown(Duration.ofSeconds(2)));
        coordinators.clear();
    }

    @Test
    void distributesDurableRunsWithoutExceedingCapacityAndMeasuresNormalizedUtilization() throws Exception {
        List<JobRun> pendingRuns = new ArrayList<>();
        for (int index = 0; index < 18; index++) {
            UUID definitionId = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                    .withName("hybrid-distribution-" + UUID.randomUUID())
                    .withDefaultDatasourceReferences()
                    .build()).id();
            pendingRuns.add(jobRunRepository.insertPendingNow(definitionId, null, 1));
        }

        ActiveRunRegistry firstRegistry = new ActiveRunRegistry();
        ActiveRunRegistry secondRegistry = new ActiveRunRegistry();
        WorkerBusySlotTracker firstTracker = tracker("hybrid-worker-one", 1);
        WorkerBusySlotTracker secondTracker = tracker("hybrid-worker-two", 2);
        AtomicInteger firstExecutions = new AtomicInteger();
        AtomicInteger secondExecutions = new AtomicInteger();
        JobExecutionService firstExecution = executionService(firstRegistry, 50, firstExecutions);
        JobExecutionService secondExecution = executionService(secondRegistry, 50, secondExecutions);
        WorkerDispatchCoordinator first = coordinator("hybrid-worker-one", 1, firstExecution,
                firstRegistry, firstTracker);
        WorkerDispatchCoordinator second = coordinator("hybrid-worker-two", 2, secondExecution,
                secondRegistry, secondTracker);

        first.requestGenericRefill("startup");
        second.requestGenericRefill("startup");

        await(() -> pendingRuns.stream().allMatch(run -> jobRunRepository.findById(run.id())
                .map(JobRun::status)
                .map(JobRunStatus::isTerminal)
                .orElse(false)));
        awaitStable(() -> firstTracker.activeSlots() == 0 && secondTracker.activeSlots() == 0
            && first.availableCapacity() == first.maxConcurrentRuns()
            && second.availableCapacity() == second.maxConcurrentRuns());

        assertEquals(18, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_run WHERE status = 'SUCCEEDED'", Map.of(), Integer.class));
        assertTrue(first.availableCapacity() <= first.maxConcurrentRuns());
        assertTrue(second.availableCapacity() <= second.maxConcurrentRuns());
        assertEquals(0, firstTracker.activeSlots());
        assertEquals(0, secondTracker.activeSlots());
        double firstBusy = firstTracker.normalizedBusySlotSeconds();
        double secondBusy = secondTracker.normalizedBusySlotSeconds();
        assertTrue(firstBusy > 0);
        assertTrue(secondBusy > 0);
        assertTrue(secondExecutions.get() > firstExecutions.get(),
            () -> "Expected the two-slot worker to complete more work: "
                + firstExecutions + " versus " + secondExecutions);
        double normalizedRatio = secondBusy / firstBusy;
        assertTrue(normalizedRatio > 0.60 && normalizedRatio < 1.40,
            () -> "Normalized worker utilization was not approximately balanced: "
                + firstBusy + " versus " + secondBusy
                + " (runs " + firstExecutions + " versus " + secondExecutions + ")");
    }

    private WorkerDispatchCoordinator coordinator(String identity, int capacity,
                                                   JobExecutionService executionService,
                                                   ActiveRunRegistry registry,
                                                   WorkerBusySlotTracker tracker) {
        ManagedRuntimeMetrics metrics = new ManagedRuntimeMetrics(new SimpleMeterRegistry());
        return register(new WorkerDispatchCoordinator(
                runLeaseService, jobRunRepository, executionService, registry,
                heartbeatService(), new WorkerRunIdentity(identity), capacity, LEASE_DURATION,
                Duration.ofSeconds(2), metrics, policy(), new WorkerAdmissionScheduler(), tracker, 1_024));
    }

    private WorkerBusySlotTracker tracker(String identity, int capacity) {
        return new WorkerBusySlotTracker(new SimpleMeterRegistry(), identity, capacity, System::nanoTime);
    }

    private WorkerDispatchCoordinator register(WorkerDispatchCoordinator coordinator) {
        coordinators.add(coordinator);
        return coordinator;
    }

    private JobExecutionService executionService(ActiveRunRegistry registry, long durationMillis,
                                                 AtomicInteger executionCount) {
        JobExecutionService executionService = mock(JobExecutionService.class);
        doAnswer(invocation -> {
            ClaimedRunPreparation preparation = invocation.getArgument(0);
            JobRun run = preparation.run();
            RunExecutionHandle handle = new RunExecutionHandle(run, options());
            registry.register(handle);
            Consumer<RunExecutionHandle> onStarted = invocation.getArgument(1);
            onStarted.accept(handle);
            executionCount.incrementAndGet();
            Thread.sleep(durationMillis);
            runFinalizationService.markSucceeded(run.id(), run.leaseToken(), 1, durationMillis, null);
            return new JobRunOutcome(run.id(), JobRunStatus.SUCCEEDED, 1, durationMillis);
        }).when(executionService).executeClaimedRun(any(), any());
        return executionService;
    }

    private org.replicadb.server.job.execution.HeartbeatService heartbeatService() {
        org.replicadb.server.job.execution.HeartbeatService heartbeat = mock(
                org.replicadb.server.job.execution.HeartbeatService.class);
        when(heartbeat.start(any())).thenReturn(new HeartbeatHandle(() -> { }));
        return heartbeat;
    }

    private static WorkerAdmissionPolicy policy() {
        WorkerRuntimeProperties.Admission admission = new WorkerRuntimeProperties.Admission();
        admission.setJitterMax(Duration.ZERO);
        admission.setGenericCooldown(Duration.ofMillis(100));
        admission.getAdaptiveBackoff().setEnabled(false);
        return new WorkerAdmissionPolicy(admission, System::nanoTime, () -> 0.0);
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

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline && !check.completed()) {
            Thread.sleep(20);
        }
        assertTrue(check.completed());
    }

    private static void awaitStable(Check check) throws Exception {
        AtomicInteger stableSamples = new AtomicInteger();
        await(() -> {
            if (check.completed()) {
                return stableSamples.incrementAndGet() >= 5;
            }
            stableSamples.set(0);
            return false;
        });
    }

    @FunctionalInterface
    private interface Check {
        boolean completed();
    }
}
