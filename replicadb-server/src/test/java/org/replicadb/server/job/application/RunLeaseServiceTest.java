package org.replicadb.server.job.application;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.LeaseToken;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.domain.ClaimedRunPreparation;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunLeaseServiceTest {

    private final JobRunStore runStore = mock(JobRunStore.class);
    private final RunLeaseService service = new RunLeaseService(runStore);

    @Test
    void delegatesQueueClaimWithoutRequestedRunId() {
        Duration leaseDuration = Duration.ofMinutes(5);
        when(runStore.claimNextEligible(isNull(), eq("worker-1"), eq(leaseDuration)))
                .thenReturn(Optional.empty());

        assertEquals(Optional.empty(), service.claimNextEligible("worker-1", leaseDuration));

        verify(runStore).claimNextEligible(isNull(), eq("worker-1"), eq(leaseDuration));
    }

    @Test
    void delegatesDirectedClaimWithRequestedRunId() {
        UUID runId = UUID.randomUUID();
        Duration leaseDuration = Duration.ofMinutes(5);
        when(runStore.claimNextEligible(runId, "api", leaseDuration)).thenReturn(Optional.empty());

        assertEquals(Optional.empty(), service.claimRequested(runId, "api", leaseDuration));

        verify(runStore).claimNextEligible(runId, "api", leaseDuration);
    }

    @Test
    void validatesLeaseAndExecutorInputsBeforeCallingStore() {
        assertThrows(IllegalArgumentException.class,
                () -> service.claimNextEligible("worker", Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> service.claimNextEligible(" ", Duration.ofMinutes(1)));
        assertThrows(IllegalArgumentException.class,
                () -> service.renewLease(UUID.randomUUID(), LeaseToken.generate(), Duration.ZERO));
    }

    @Test
    void delegatesLeaseRenewalWithCurrentToken() {
        UUID runId = UUID.randomUUID();
        LeaseToken token = LeaseToken.generate();
        Duration leaseDuration = Duration.ofMinutes(5);
        when(runStore.renewLease(runId, token, leaseDuration))
                .thenReturn(JobRunStore.LeaseRenewalResult.RENEWED);

        assertEquals(JobRunStore.LeaseRenewalResult.RENEWED,
                service.renewLease(runId, token, leaseDuration));
    }

        @Test
        void delegatesPreparedQueueClaimWithoutRequestedRunId() {
                Duration leaseDuration = Duration.ofMinutes(5);
                when(runStore.claimAndPrepare(isNull(), eq("worker-1"), eq(leaseDuration)))
                                .thenReturn(Optional.empty());

                assertEquals(Optional.empty(), service.claimAndPrepare(null, "worker-1", leaseDuration));

                verify(runStore).claimAndPrepare(isNull(), eq("worker-1"), eq(leaseDuration));
        }

        @Test
        void delegatesPreparedDirectedClaimWithRequestedRunId() {
                UUID runId = UUID.randomUUID();
                Duration leaseDuration = Duration.ofMinutes(5);
                when(runStore.claimAndPrepare(runId, "worker-1", leaseDuration))
                                .thenReturn(Optional.empty());

                assertEquals(Optional.empty(), service.claimAndPrepare(runId, "worker-1", leaseDuration));

                verify(runStore).claimAndPrepare(runId, "worker-1", leaseDuration);
        }

        @Test
        void validatesPreparedClaimInputsBeforeCallingStore() {
                assertThrows(IllegalArgumentException.class,
                                () -> service.claimAndPrepare(null, "worker", Duration.ZERO));
                assertThrows(IllegalArgumentException.class,
                                () -> service.claimAndPrepare(null, " ", Duration.ofMinutes(1)));
        }
}
