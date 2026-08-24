package org.replicadb.server.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class HeartbeatServiceIT {

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private RunLeaseService runLeaseService;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
    }

    @Test
    void renewsDatabaseLeaseWhileAReplicadbOperationIsBlocked() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun claimed = jobRunRepository.claimNextEligible(
                jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
                "heartbeat-worker", Duration.ofSeconds(5)).orElseThrow();
        RunExecutionHandle executionHandle = executionHandle(claimed);
        HeartbeatService service = heartbeatService();
        CountDownLatch operation = new CountDownLatch(1);

        try {
            HeartbeatHandle heartbeat = service.start(executionHandle);
            await(() -> {
                JobRun current = jobRunRepository.findById(claimed.id()).orElseThrow();
                return current.heartbeatAt().isAfter(claimed.heartbeatAt())
                        && current.leaseUntil().isAfter(claimed.leaseUntil());
            });
            assertTrue(operation.getCount() == 1);
            heartbeat.stop();
        } finally {
            operation.countDown();
            service.shutdown();
        }
    }

    @Test
    void fencesAnExpiredLeaseAndCancelsTheLocalExecutionContext() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun claimed = jobRunRepository.claimNextEligible(
                jobRunRepository.insertPendingNow(definition.id(), null, 1).id(),
                "stale-heartbeat-worker", Duration.ofMinutes(5)).orElseThrow();
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
                Map.of("id", claimed.id()));
        RunExecutionHandle executionHandle = executionHandle(claimed);
        HeartbeatService service = heartbeatService();

        try {
            HeartbeatHandle heartbeat = service.start(executionHandle);
            await(() -> heartbeat.isStopped() && executionHandle.cancellationContext().isCancellationRequested());
            assertEquals(JobRunStatus.RUNNING, jobRunRepository.findById(claimed.id()).orElseThrow().status());
        } finally {
            service.shutdown();
        }
    }

    private HeartbeatService heartbeatService() {
        return new HeartbeatService(runLeaseService, Duration.ofMillis(20), Duration.ofSeconds(5),
                Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(2));
    }

    private static RunExecutionHandle executionHandle(JobRun run) throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlite:source.db",
                "--sink-connect", "jdbc:sqlite:sink.db"
        });
        return new RunExecutionHandle(run, options);
    }

    private static JobDefinition definition() {
        return JobDefinitionTestFixtures.aJobDefinition()
                .withName("heartbeat-job-" + UUID.randomUUID())
                .build();
    }

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && !check.completed()) {
            Thread.sleep(10);
        }
        assertTrue(check.completed());
    }

    @FunctionalInterface
    private interface Check {
        boolean completed();
    }
}