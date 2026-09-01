package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.domain.ManagedDataSourceTestFixtures;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.application.RunRecoveryResult;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobRunRepositoryIT {

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private ManagedDataSourceRepository managedDataSourceRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_run, job_definition CASCADE", Map.of());
        jdbcTemplate.update("TRUNCATE TABLE datasource_permission, managed_datasource CASCADE", Map.of());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.source());
        managedDataSourceRepository.insert(ManagedDataSourceTestFixtures.sink());
    }

    @Test
        void insertsPendingRunUsingDatabaseTimeAndClaimsItImmediately() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());

        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        Map<String, Object> timestamps = jdbcTemplate.getJdbcTemplate().queryForMap("""
            SELECT available_at <= now() AS available_now,
                   created_at <= now() AS created_now
            FROM job_run WHERE id = ?
            """, pending.id());

        assertTrue(Boolean.TRUE.equals(timestamps.get("available_now")), timestamps.toString());
        assertTrue(Boolean.TRUE.equals(timestamps.get("created_now")), timestamps.toString());
        assertEquals(JobRunStatus.RUNNING,
            jobRunRepository.claimNextEligible(pending.id(), "worker-now", Duration.ofMinutes(5))
                .orElseThrow().status());
        }

        @Test
    void claimsPendingRunAndSetsRunningFields() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);

        Map<String, Object> availability = jdbcTemplate.getJdbcTemplate().queryForMap("""
            SELECT status, available_at, now() AS database_now,
                   available_at <= now() AS eligible
            FROM job_run WHERE id = ?
            """, pending.id());
        assertTrue(Boolean.TRUE.equals(availability.get("eligible")), availability.toString());

        JobRun claimed = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertEquals(pending.id(), claimed.id());
        assertEquals(JobRunStatus.RUNNING, claimed.status());
        assertEquals("worker-1", claimed.executorIdentity());
        assertTrue(claimed.startedAt() != null);
        assertTrue(claimed.heartbeatAt() != null);
        assertTrue(claimed.leaseUntil() != null);
        assertNotNull(claimed.availableAt());
        assertNotNull(claimed.leaseToken());
    }

        @Test
        void claimsOnlyEligibleRunsAndAssignsDistinctLeaseTokens() {
        JobDefinition eligibleDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition futureDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondEligibleDefinition = jobDefinitionRepository.insert(definition());
        JobRun eligible = jobRunRepository.insertPendingNow(eligibleDefinition.id(), null, 1);
        JobRun future = jobRunRepository.insertPendingNow(futureDefinition.id(), null, 1);
        JobRun secondEligible = jobRunRepository.insertPendingNow(secondEligibleDefinition.id(), null, 1);
        jdbcTemplate.update("UPDATE job_run SET available_at = now() + interval '300 seconds' WHERE id = :id",
            Map.of("id", future.id()));
        jdbcTemplate.update("UPDATE job_run SET available_at = now() - interval '1 second' WHERE id = :id",
            Map.of("id", eligible.id()));
        jdbcTemplate.update("UPDATE job_run SET available_at = now() - interval '1 second' WHERE id = :id",
            Map.of("id", secondEligible.id()));

        JobRun firstClaim = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5))
            .orElseThrow();
        JobRun secondClaim = jobRunRepository.claimNextEligible(null, "worker-2", Duration.ofMinutes(5))
            .orElseThrow();

        assertEquals(eligible.id(), firstClaim.id());
        assertNotEquals(firstClaim.id(), secondClaim.id());
        assertNotNull(firstClaim.leaseToken());
        assertNotNull(secondClaim.leaseToken());
        assertNotEquals(firstClaim.leaseToken(), secondClaim.leaseToken());
        assertEquals(JobRunStatus.PENDING, jobRunRepository.findById(future.id()).orElseThrow().status());
        assertTrue(jobRunRepository.claimNextEligible(null, "worker-3", Duration.ofMinutes(5)).isEmpty());
        }

        @Test
        void directedClaimDoesNotClaimAnotherRunOrAnIneligibleRun() {
        JobDefinition firstDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondDefinition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPendingNow(firstDefinition.id(), null, 1);
        JobRun second = jobRunRepository.insertPendingNow(secondDefinition.id(), null, 1);
        jdbcTemplate.update("UPDATE job_run SET available_at = now() + interval '300 seconds' WHERE id = :id",
            Map.of("id", second.id()));

        assertTrue(jobRunRepository.claimNextEligible(second.id(), "worker-1", Duration.ofMinutes(5)).isEmpty());
        JobRun claimed = jobRunRepository.claimNextEligible(first.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();

        assertEquals(first.id(), claimed.id());
        assertEquals(JobRunStatus.PENDING, jobRunRepository.findById(second.id()).orElseThrow().status());
        assertTrue(jobRunRepository.claimNextEligible(first.id(), "worker-2", Duration.ofMinutes(5)).isEmpty());
        }

        @Test
        void renewsAnUnexpiredLeaseOwnedByTheCurrentToken() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();

        JobRunStore.LeaseRenewalResult result = jobRunRepository.renewLease(
            claimed.id(), claimed.leaseToken(), Duration.ofMinutes(10));

        JobRun renewed = jobRunRepository.findById(claimed.id()).orElseThrow();
        assertEquals(JobRunStore.LeaseRenewalResult.RENEWED, result);
        assertTrue(renewed.leaseUntil().isAfter(claimed.leaseUntil()));
        assertTrue(renewed.heartbeatAt().isAfter(claimed.heartbeatAt()));
        }

        @Test
        void fencesStaleTokensAndMissingRuns() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();

        assertEquals(JobRunStore.LeaseRenewalResult.FENCED,
            jobRunRepository.renewLease(claimed.id(), LeaseToken.generate(), Duration.ofMinutes(5)));
        assertEquals(JobRunStore.LeaseRenewalResult.NOT_FOUND,
            jobRunRepository.renewLease(UUID.randomUUID(), claimed.leaseToken(), Duration.ofMinutes(5)));
        }

        @Test
        void refusesRenewalAfterExpiryOrTerminalTransition() {
        JobDefinition expiredDefinition = jobDefinitionRepository.insert(definition());
        JobRun expiredPending = jobRunRepository.insertPendingNow(expiredDefinition.id(), null, 1);
        JobRun expired = jobRunRepository.claimNextEligible(expiredPending.id(), "worker-1",
            Duration.ofMinutes(5)).orElseThrow();
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
            Map.of("id", expired.id()));

        assertEquals(JobRunStore.LeaseRenewalResult.FENCED,
            jobRunRepository.renewLease(expired.id(), expired.leaseToken(), Duration.ofMinutes(5)));

        JobDefinition terminalDefinition = jobDefinitionRepository.insert(definition());
        JobRun terminalPending = jobRunRepository.insertPendingNow(terminalDefinition.id(), null, 1);
        JobRun terminal = jobRunRepository.claimNextEligible(terminalPending.id(), "worker-2",
            Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(terminal.id(), terminal.leaseToken(), 0, 0, null);

        assertEquals(JobRunStore.LeaseRenewalResult.FENCED,
            jobRunRepository.renewLease(terminal.id(), terminal.leaseToken(), Duration.ofMinutes(5)));
        }

        @Test
        void recoversExpiredRunAsANewBackoffAttempt() {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
            .withDefaultDatasourceReferences()
            .withMode(ReplicationMode.INCREMENTAL)
            .withIncrementalWatermarkColumn("updated_at")
            .withRetryPolicy(new RetryPolicy(3, 10, true))
            .build());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();
        expire(claimed);

        RunRecoveryResult result = jobRunRepository.recoverExpiredRun(claimed.id());

        JobRun abandoned = result.abandonedRun().orElseThrow();
        JobRun replacement = result.replacementRun().orElseThrow();
        assertEquals(JobRunStatus.RETRY_SCHEDULED, abandoned.status());
        assertEquals(JobRunStatus.PENDING, replacement.status());
        assertEquals(claimed.id(), replacement.previousRunId());
        assertEquals(2, replacement.attempt());
        assertTrue(replacement.availableAt().isAfter(Instant.now()));
        assertTrue(jobRunRepository.claimNextEligible(replacement.id(), "worker-2", Duration.ofMinutes(5))
            .isEmpty());

        jdbcTemplate.update("UPDATE job_run SET available_at = now() - interval '1 second' WHERE id = :id",
            Map.of("id", replacement.id()));
        assertTrue(jobRunRepository.claimNextEligible(replacement.id(), "worker-2", Duration.ofMinutes(5))
            .isPresent());
        }

        @Test
        void recoversAnExpiredRunOnlyOnceWhenScansRace() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
            .withDefaultDatasourceReferences()
            .withMode(ReplicationMode.INCREMENTAL)
            .withIncrementalWatermarkColumn("updated_at")
            .withRetryPolicy(new RetryPolicy(3, 0, true))
            .build());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();
        expire(claimed);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        try {
            Future<RunRecoveryResult> first = executor.submit(() -> recoverAfter(start, claimed.id()));
            Future<RunRecoveryResult> second = executor.submit(() -> recoverAfter(start, claimed.id()));
            start.countDown();

            RunRecoveryResult firstResult = first.get(5, TimeUnit.SECONDS);
            RunRecoveryResult secondResult = second.get(5, TimeUnit.SECONDS);
            int replacements = (firstResult.replacementCreated() ? 1 : 0)
                + (secondResult.replacementCreated() ? 1 : 0);

            assertEquals(1, replacements);
            assertEquals(1, jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM job_run
                WHERE job_definition_id = :jobDefinitionId AND status = 'PENDING'
                """, Map.of("jobDefinitionId", definition.id()), Integer.class));
        } finally {
            executor.shutdownNow();
        }
        }

        @Test
        void marksExpiredRunsFailedWhenRetryIsDisabledOrExhausted() {
        JobDefinition completeDefinition = jobDefinitionRepository.insert(
            JobDefinitionTestFixtures.aJobDefinition().withDefaultDatasourceReferences()
                .withMode(ReplicationMode.COMPLETE).build());
        JobRun completePending = jobRunRepository.insertPendingNow(completeDefinition.id(), null, 1);
        JobRun completeClaimed = jobRunRepository.claimNextEligible(completePending.id(), "worker-1",
            Duration.ofMinutes(5)).orElseThrow();
        expire(completeClaimed);

        RunRecoveryResult completeResult = jobRunRepository.recoverExpiredRun(completeClaimed.id());

        assertTrue(completeResult.replacementRun().isEmpty());
        assertEquals(JobRunStatus.FAILED,
            jobRunRepository.findById(completeClaimed.id()).orElseThrow().status());
        assertEquals("Lease expired before execution completed",
            jobRunRepository.findById(completeClaimed.id()).orElseThrow().errorMessage());

        JobDefinition exhaustedDefinition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
            .withDefaultDatasourceReferences()
            .withMode(ReplicationMode.INCREMENTAL)
            .withIncrementalWatermarkColumn("updated_at")
            .withRetryPolicy(new RetryPolicy(1, 0, true))
            .build());
        JobRun exhaustedPending = jobRunRepository.insertPendingNow(exhaustedDefinition.id(), null, 1);
        JobRun exhaustedClaimed = jobRunRepository.claimNextEligible(exhaustedPending.id(), "worker-2",
            Duration.ofMinutes(5)).orElseThrow();
        expire(exhaustedClaimed);

        assertTrue(jobRunRepository.recoverExpiredRun(exhaustedClaimed.id()).replacementRun().isEmpty());
        assertEquals(JobRunStatus.FAILED,
            jobRunRepository.findById(exhaustedClaimed.id()).orElseThrow().status());
        }

        @Test
        void cancellationWinsOverExpiredLeaseRecovery() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();
        assertEquals(JobRunStore.CancellationResult.REQUESTED,
            jobRunRepository.requestCancellation(claimed.id(), "sink warning"));
        expire(claimed);

        RunRecoveryResult result = jobRunRepository.recoverExpiredRun(claimed.id());

        assertTrue(result.replacementRun().isEmpty());
        JobRun cancelled = result.abandonedRun().orElseThrow();
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
        assertEquals("sink warning", cancelled.cancellationWarning());
        }

        @Test
        void scansExpiredRunsAndOwnedCancellationRequestsWithLimits() {
        JobDefinition expiredRunningDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition expiredCancellationDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition liveDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition otherWorkerDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition terminalDefinition = jobDefinitionRepository.insert(definition());

        JobRun expiredRunning = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(expiredRunningDefinition.id(), null, 1).id(),
            "worker-one", Duration.ofMinutes(5)).orElseThrow();
        expire(expiredRunning);

        JobRun expiredCancellation = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(expiredCancellationDefinition.id(), null, 1).id(),
            "worker-one", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.requestCancellation(expiredCancellation.id(), "cancel warning");
        expire(expiredCancellation);

        JobRun live = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(liveDefinition.id(), null, 1).id(),
            "worker-one", Duration.ofMinutes(5)).orElseThrow();
        JobRun otherWorker = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(otherWorkerDefinition.id(), null, 1).id(),
            "worker-two", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.requestCancellation(otherWorker.id(), "other warning");
        JobRun terminal = jobRunRepository.claimNextEligible(
            jobRunRepository.insertPendingNow(terminalDefinition.id(), null, 1).id(),
            "worker-three", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(terminal.id(), terminal.leaseToken(), 0, 0, null);

        java.util.List<UUID> expired = jobRunRepository.findExpiredRunIds(10);
        assertTrue(expired.contains(expiredRunning.id()));
        assertTrue(expired.contains(expiredCancellation.id()));
        assertTrue(!expired.contains(live.id()));
        assertTrue(!expired.contains(terminal.id()));
        assertEquals(1, jobRunRepository.findExpiredRunIds(1).size());

        assertEquals(java.util.List.of(expiredCancellation.id()),
            jobRunRepository.findCancellationRequestedRunIds("worker-one", 10));
        assertEquals(java.util.List.of(otherWorker.id()),
            jobRunRepository.findCancellationRequestedRunIds("worker-two", 10));
        assertTrue(jobRunRepository.findCancellationRequestedRunIds("worker-one", 1).size() <= 1);
        }

        @Test
        void ignoresMissingAndNonExpiredRuns() {
        assertTrue(jobRunRepository.recoverExpiredRun(UUID.randomUUID()).abandonedRun().isEmpty());

        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();

        RunRecoveryResult result = jobRunRepository.recoverExpiredRun(claimed.id());

        assertTrue(result.abandonedRun().isEmpty());
        assertEquals(JobRunStatus.RUNNING, jobRunRepository.findById(claimed.id()).orElseThrow().status());
        }

        @Test
        void fencesProgressTerminalWritesAndWatermarksAfterRecovery() {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
            .withDefaultDatasourceReferences()
            .withMode(ReplicationMode.INCREMENTAL)
            .withIncrementalWatermarkColumn("updated_at")
            .withRetryPolicy(new RetryPolicy(3, 0, true))
            .build());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();

        assertEquals(JobRunStore.FencedUpdateResult.UPDATED,
            jobRunRepository.recordProgress(claimed.id(), claimed.leaseToken(), 4, 12));

        expire(claimed);
        JobRun replacement = jobRunRepository.recoverExpiredRun(claimed.id()).replacementRun().orElseThrow();
        LeaseToken staleToken = claimed.leaseToken();

        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
            jobRunRepository.recordProgress(claimed.id(), staleToken, 99, 99));
        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
            jobRunRepository.markFailed(claimed.id(), staleToken, 99, 99, "stale failure"));
        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
            jobRunRepository.markCancelled(claimed.id(), staleToken, 99, 99));
        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
            jobRunRepository.markSucceeded(claimed.id(), staleToken, 99, 99, "999"));
        assertTrue(jobRunRepository.findLastCommittedWatermark(definition.id()).isEmpty());
        assertEquals(JobRunStatus.PENDING, jobRunRepository.findById(replacement.id()).orElseThrow().status());
        }

        @Test
        void allowsCurrentTokenToFinalizeAndPreservesCancellationWarning() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
            .orElseThrow();

        assertEquals(JobRunStore.FencedUpdateResult.UPDATED,
            jobRunRepository.markSucceeded(claimed.id(), claimed.leaseToken(), 4, 12, "42"));
        assertEquals("42", jobRunRepository.findLastCommittedWatermark(definition.id()).orElseThrow());

        JobDefinition cancelledDefinition = jobDefinitionRepository.insert(definition());
        JobRun cancelledPending = jobRunRepository.insertPendingNow(cancelledDefinition.id(), null, 1);
        JobRun cancelledClaimed = jobRunRepository.claimNextEligible(cancelledPending.id(), "worker-2",
            Duration.ofMinutes(5)).orElseThrow();
        assertEquals(JobRunStore.CancellationResult.REQUESTED,
            jobRunRepository.requestCancellation(cancelledClaimed.id(), "sink warning"));

        assertEquals(JobRunStore.FencedUpdateResult.UPDATED,
            jobRunRepository.markCancelled(cancelledClaimed.id(), cancelledClaimed.leaseToken(), 2, 8));
        JobRun cancelled = jobRunRepository.findById(cancelledClaimed.id()).orElseThrow();
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
        assertEquals("sink warning", cancelled.cancellationWarning());
        }

    @Test
    void skipsALockedPendingRowAndClaimsTheNextOne() throws Exception {
        JobDefinition firstDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondDefinition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPendingNow(firstDefinition.id(), null, 1);
        JobRun second = jobRunRepository.insertPendingNow(secondDefinition.id(), null, 1);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            UUID lockedId;
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT id FROM job_run ORDER BY created_at, id LIMIT 1 FOR UPDATE")) {
                assertTrue(resultSet.next());
                lockedId = (UUID) resultSet.getObject("id");
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<JobRun> claimedFuture = executor.submit(
                        () -> jobRunRepository.claimNextEligible(null, "worker-2", Duration.ofMinutes(5))
                                .orElseThrow());
                JobRun claimed = claimedFuture.get(2, TimeUnit.SECONDS);

                assertEquals(second.id(), claimed.id());
                assertEquals(JobRunStatus.RUNNING, claimed.status());
                assertNotEquals(first.id(), claimed.id());
                assertNotEquals(lockedId, claimed.id());
            } finally {
                executor.shutdownNow();
            }
            connection.commit();
        }
    }

    @Test
    void returnsEmptyWhenNoPendingRunExists() {
        assertTrue(jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).isEmpty());
    }

    @Test
    void rejectsIllegalTransitionAfterCancellation() {
        JobDefinition pendingDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition runningDefinition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(pendingDefinition.id(), null, 1);
        jobRunRepository.insertPendingNow(runningDefinition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).orElseThrow();

        jobRunRepository.markCancelled(running.id(), running.leaseToken(), 4, 12);

        assertEquals(JobRunStore.FencedUpdateResult.FENCED,
            jobRunRepository.markSucceeded(pending.id(), LeaseToken.generate(), 4, 12, "42"));
    }

    @Test
    void claimsOnlyTheRequestedPendingRun() {
        JobDefinition firstDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondDefinition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPendingNow(firstDefinition.id(), null, 1);
        JobRun second = jobRunRepository.insertPendingNow(secondDefinition.id(), null, 1);

        JobRun claimed = jobRunRepository.claimNextEligible(second.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertEquals(second.id(), claimed.id());
        assertEquals(JobRunStatus.PENDING,
                jobRunRepository.findById(first.id()).orElseThrow().status());
        assertEquals(JobRunStatus.RUNNING, claimed.status());
    }

        @Test
        void claimsAndPreparesEncryptedDatasourceSnapshotsAtClaimTime() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);

        ClaimedRunPreparation preparation = jobRunRepository.claimAndPrepare(
            pending.id(), "worker-prepared", Duration.ofMinutes(5)).orElseThrow();

        assertEquals(pending.id(), preparation.run().id());
        assertEquals(definition.id(), preparation.definition().id());
        assertEquals(ManagedDataSourceTestFixtures.SOURCE_DATASOURCE_ID,
            preparation.sourceDataSource().id());
        assertEquals(ManagedDataSourceTestFixtures.SINK_DATASOURCE_ID,
            preparation.sinkDataSource().id());
        assertArrayEquals(new byte[]{1, 2, 3}, preparation.sourceDataSource().encryptedSecurity());
        assertArrayEquals(new byte[]{1, 2, 3}, preparation.sinkDataSource().encryptedSecurity());
        assertEquals(ManagedDataSourceTestFixtures.SOURCE_DATASOURCE_ID,
            preparation.run().resolvedSourceDatasourceId());
        assertEquals(ManagedDataSourceTestFixtures.SINK_DATASOURCE_ID,
            preparation.run().resolvedSinkDatasourceId());
        assertNotNull(preparation.run().datasourcesResolvedAt());
        assertEquals(preparation.run(), jobRunRepository.findById(pending.id()).orElseThrow());
        }

        @Test
        void doesNotClaimAJobWithADisabledDatasourceBinding() {
        JobDefinition definition = jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
            .withDefaultDatasourceReferences()
            .withSourceDatasourceUseEnabled(false)
            .build());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);

        assertTrue(jobRunRepository.claimAndPrepare(pending.id(), "worker-disabled",
            Duration.ofMinutes(5)).isEmpty());
        assertEquals(JobRunStatus.PENDING, jobRunRepository.findById(pending.id()).orElseThrow().status());
        }

        @Test
        void excludesDisabledBindingsFromEveryEligibilityPathUntilReenabled() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        jdbcTemplate.update("""
            UPDATE job_definition
            SET source_datasource_use_enabled = false,
                sink_datasource_use_enabled = false
            WHERE id = :id
            """, Map.of("id", definition.id()));

        assertTrue(jobRunRepository.claimNextEligible(pending.id(), "worker-disabled",
            Duration.ofMinutes(5)).isEmpty());
        assertTrue(jobRunRepository.claimAndPrepare(pending.id(), "worker-disabled",
            Duration.ofMinutes(5)).isEmpty());
        assertEquals(0, jobRunRepository.findEligibleRunSnapshot(10).eligibleCount());
        assertEquals(JobRunStatus.PENDING, jobRunRepository.findById(pending.id()).orElseThrow().status());

        jdbcTemplate.update("""
            UPDATE job_definition
            SET source_datasource_use_enabled = true,
                sink_datasource_use_enabled = true
            WHERE id = :id
            """, Map.of("id", definition.id()));

        assertTrue(jobRunRepository.claimAndPrepare(pending.id(), "worker-enabled",
            Duration.ofMinutes(5)).isPresent());
        }

        @Test
        void concurrentlyPreparesRunsSharingDatasourceRows() throws Exception {
        JobDefinition firstDefinition = jobDefinitionRepository.insert(definition());
        JobDefinition secondDefinition = jobDefinitionRepository.insert(definition());
        JobRun first = jobRunRepository.insertPendingNow(firstDefinition.id(), null, 1);
        JobRun second = jobRunRepository.insertPendingNow(secondDefinition.id(), null, 1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        try {
            Future<Optional<ClaimedRunPreparation>> firstFuture = executor.submit(() -> {
            start.await(2, TimeUnit.SECONDS);
            return jobRunRepository.claimAndPrepare(first.id(), "worker-one", Duration.ofMinutes(5));
            });
            Future<Optional<ClaimedRunPreparation>> secondFuture = executor.submit(() -> {
            start.await(2, TimeUnit.SECONDS);
            return jobRunRepository.claimAndPrepare(second.id(), "worker-two", Duration.ofMinutes(5));
            });
            start.countDown();

            ClaimedRunPreparation firstPreparation = firstFuture.get(5, TimeUnit.SECONDS).orElseThrow();
            ClaimedRunPreparation secondPreparation = secondFuture.get(5, TimeUnit.SECONDS).orElseThrow();

            assertNotEquals(firstPreparation.run().id(), secondPreparation.run().id());
            assertEquals(JobRunStatus.RUNNING,
                jobRunRepository.findById(first.id()).orElseThrow().status());
            assertEquals(JobRunStatus.RUNNING,
                jobRunRepository.findById(second.id()).orElseThrow().status());
        } finally {
            executor.shutdownNow();
        }
        }

    @Test
    void returnsEmptyWhenRequestedRunIsNotPendingOrDoesNotExist() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertTrue(jobRunRepository.claimNextEligible(running.id(), "worker-2", Duration.ofMinutes(5)).isEmpty());
        assertTrue(jobRunRepository.claimNextEligible(UUID.randomUUID(), "worker-2", Duration.ofMinutes(5)).isEmpty());
    }

    @Test
    void reportsOnlyActiveStatuses() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        assertTrue(jobRunRepository.hasActiveRun(definition.id()));

        JobRun running = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();
        assertTrue(jobRunRepository.hasActiveRun(definition.id()));
        assertEquals(JobRunStore.CancellationResult.REQUESTED,
            jobRunRepository.requestCancellation(running.id(), "cancel warning"));
        assertTrue(jobRunRepository.hasActiveRun(definition.id()));
        jobRunRepository.markCancelled(running.id(), running.leaseToken(), 0, 0);
        assertTrue(!jobRunRepository.hasActiveRun(definition.id()));
    }

    @Test
    void rejectsConcurrentPendingRunsForOneDefinitionButAllowsAnotherAfterTerminalState() throws Exception {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var start = new java.util.concurrent.CountDownLatch(1);
            Future<?> first = executor.submit(() -> insertAfter(start, definition.id()));
            Future<?> second = executor.submit(() -> insertAfter(start, definition.id()));
            start.countDown();

            int successes = 0;
            int failures = 0;
            for (Future<?> future : new Future<?>[]{first, second}) {
                try {
                    future.get(2, TimeUnit.SECONDS);
                    successes++;
                } catch (java.util.concurrent.ExecutionException exception) {
                    assertTrue(exception.getCause() instanceof IllegalStateException);
                    failures++;
                }
            }
            assertEquals(1, successes);
            assertEquals(1, failures);

            JobRun running = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).orElseThrow();
            jobRunRepository.markSucceeded(running.id(), running.leaseToken(), 0, 0, null);
            assertTrue(!jobRunRepository.hasActiveRun(definition.id()));
            jobRunRepository.insertPendingNow(definition.id(), null, 2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void markCancelRequestedIsIdempotentAfterTerminalTransition() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(running.id(), running.leaseToken(), 0, 0, null);

        assertEquals(JobRunStore.CancellationResult.TERMINAL,
            jobRunRepository.requestCancellation(running.id(), "ignored warning"));
        JobRun unchanged = jobRunRepository.findById(running.id()).orElseThrow();
        assertEquals(JobRunStatus.SUCCEEDED, unchanged.status());
        assertNull(unchanged.cancellationWarning());
    }

    @Test
    void cancelsAPendingRunWithoutClaimingIt() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);

        assertEquals(JobRunStore.CancellationResult.CANCELLED,
            jobRunRepository.cancelPending(pending.id(), "pending warning"));

        JobRun cancelled = jobRunRepository.findById(pending.id()).orElseThrow();
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
        assertEquals(0, cancelled.rowsProcessed());
        assertEquals("pending warning", cancelled.cancellationWarning());
    }

    @Test
    void preservesCancellationWarningWhenExecutorFinishesCancellation() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun running = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertEquals(JobRunStore.CancellationResult.REQUESTED,
            jobRunRepository.requestCancellation(running.id(), "indeterminate sink warning"));
        assertEquals("indeterminate sink warning",
                jobRunRepository.findById(running.id()).orElseThrow().cancellationWarning());

        jobRunRepository.markCancelled(running.id(), running.leaseToken(), 0, 0);

        JobRun cancelled = jobRunRepository.findById(running.id()).orElseThrow();
        assertEquals(JobRunStatus.CANCELLED, cancelled.status());
        assertEquals("indeterminate sink warning", cancelled.cancellationWarning());
    }

    @Test
    void leavesCancellationWarningNullForUncancelledRun() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);

        assertNull(jobRunRepository.findById(pending.id()).orElseThrow().cancellationWarning());
    }

    @Test
    void paginatesRunsAndFiltersByDefinitionAndStatus() {
        UUID filteredDefinitionId = null;
        for (int index = 0; index < 5; index++) {
            JobDefinition definition = jobDefinitionRepository.insert(definition());
            if (index == 0) {
                filteredDefinitionId = definition.id();
            }
            JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
            JobRun running = jobRunRepository.claimNextEligible(pending.id(), "worker-" + index,
                    Duration.ofMinutes(5)).orElseThrow();
            if (index % 2 == 0) {
                jobRunRepository.markSucceeded(running.id(), running.leaseToken(), index, index, null);
            } else {
                jobRunRepository.markFailed(running.id(), running.leaseToken(), index, index, "failure");
            }
        }

        assertEquals(5, jobRunRepository.count(null, null, null));
        assertEquals(2, jobRunRepository.findPage(null, JobRunStatus.FAILED, 0, 2, null).size());
        assertEquals(3, jobRunRepository.count(null, JobRunStatus.SUCCEEDED, null));
        assertEquals(1, jobRunRepository.count(filteredDefinitionId, null, null));

        java.util.List<JobRun> firstPage = jobRunRepository.findPage(null, null, 0, 2, null);
        java.util.List<JobRun> secondPage = jobRunRepository.findPage(null, null, 1, 2, null);
        java.util.List<JobRun> thirdPage = jobRunRepository.findPage(null, null, 2, 2, null);
        assertEquals(2, firstPage.size());
        assertEquals(2, secondPage.size());
        assertEquals(1, thirdPage.size());
        assertEquals(5, java.util.stream.Stream.of(firstPage, secondPage, thirdPage)
                .mapToInt(java.util.List::size).sum());

        assertEquals(1, jobRunRepository.count(null, null, Set.of(filteredDefinitionId)));
        assertEquals(1, jobRunRepository.findPage(null, null, 0, 10, Set.of(filteredDefinitionId)).size());
        assertEquals(0, jobRunRepository.count(null, null, Set.of()));
        assertTrue(jobRunRepository.findPage(null, null, 0, 10, Set.of()).isEmpty());
    }

    @Test
    void findsLastCommittedWatermarkOnlyFromSuccessfulRuns() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        jobRunRepository.insertPendingNow(definition.id(), null, 1);

        assertTrue(jobRunRepository.findLastCommittedWatermark(definition.id()).isEmpty());

        JobRun running = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markSucceeded(running.id(), running.leaseToken(), 4, 12, "42");

        assertEquals("42", jobRunRepository.findLastCommittedWatermark(definition.id()).orElseThrow());
    }

    @Test
    void roundTripsResolvedDatasourceCorrelationFields() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun claimed = jobRunRepository.claimNextEligible(pending.id(), "worker-1", Duration.ofMinutes(5))
                .orElseThrow();

        jdbcTemplate.update("""
                UPDATE job_run
                SET resolved_source_datasource_id = :sourceId,
                    resolved_sink_datasource_id = :sinkId,
                    datasources_resolved_at = now()
                WHERE id = :id
                """, Map.of("sourceId", ManagedDataSourceTestFixtures.SOURCE_DATASOURCE_ID,
                "sinkId", ManagedDataSourceTestFixtures.SINK_DATASOURCE_ID, "id", claimed.id()));

        JobRun found = jobRunRepository.findById(claimed.id()).orElseThrow();

        assertEquals(ManagedDataSourceTestFixtures.SOURCE_DATASOURCE_ID, found.resolvedSourceDatasourceId());
        assertEquals(ManagedDataSourceTestFixtures.SINK_DATASOURCE_ID, found.resolvedSinkDatasourceId());
        assertNotNull(found.datasourcesResolvedAt());
    }

    @Test
    void schedulesRetryAsANewPendingRun() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());
        jobRunRepository.insertPendingNow(definition.id(), null, 1);
        JobRun failed = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).orElseThrow();
        jobRunRepository.markFailed(failed.id(), failed.leaseToken(), 4, 12, "temporary failure");

        JobRun retry = jobRunRepository.scheduleRetryNow(failed.id());

        assertEquals(JobRunStatus.RETRY_SCHEDULED,
                jobRunRepository.findById(failed.id()).orElseThrow().status());
        assertEquals(JobRunStatus.PENDING, retry.status());
        assertEquals(failed.id(), retry.previousRunId());
        assertEquals(2, retry.attempt());
        assertTrue(jdbcTemplate.queryForObject("""
            SELECT available_at <= now() FROM job_run WHERE id = :id
            """, Map.of("id", retry.id()), Boolean.class));
        assertEquals(JobRunStatus.RUNNING,
            jobRunRepository.claimNextEligible(retry.id(), "retry-worker", Duration.ofMinutes(5))
                .orElseThrow().status());
    }

    @Test
    void rejectsRetryForNonFailedRuns() {
        JobDefinition definition = jobDefinitionRepository.insert(definition());

        JobRun pending = jobRunRepository.insertPendingNow(definition.id(), null, 1);
        assertThrows(IllegalStateException.class, () -> jobRunRepository.scheduleRetryNow(pending.id()));

        JobRun running = jobRunRepository.claimNextEligible(null, "worker-1", Duration.ofMinutes(5)).orElseThrow();
        assertThrows(IllegalStateException.class, () -> jobRunRepository.scheduleRetryNow(running.id()));

        jobRunRepository.markSucceeded(running.id(), running.leaseToken(), 4, 12, "42");
        assertThrows(IllegalStateException.class, () -> jobRunRepository.scheduleRetryNow(running.id()));
    }

    private static JobDefinition definition() {
        return JobDefinitionTestFixtures.aJobDefinition()
            .withName("job-" + UUID.randomUUID())
            .withDefaultDatasourceReferences()
            .withMode(ReplicationMode.INCREMENTAL)
            .withIncrementalWatermarkColumn("updated_at")
            .withInitialWatermarkValue("0")
            .build();
    }

    private void expire(JobRun run) {
        jdbcTemplate.update("UPDATE job_run SET lease_until = now() - interval '1 second' WHERE id = :id",
                Map.of("id", run.id()));
    }

    private RunRecoveryResult recoverAfter(java.util.concurrent.CountDownLatch start, UUID runId) {
        try {
            start.await(2, TimeUnit.SECONDS);
            return jobRunRepository.recoverExpiredRun(runId);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    private void insertAfter(java.util.concurrent.CountDownLatch start, UUID definitionId) {
        try {
            start.await(2, TimeUnit.SECONDS);
            jobRunRepository.insertPendingNow(definitionId, null, 1);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }
}
