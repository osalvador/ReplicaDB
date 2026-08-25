package org.replicadb.server.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedRuntimeMetricsTest {

    @Test
    void recordsRequiredEventsWithBoundedTagsAndGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ManagedRuntimeMetrics metrics = new ManagedRuntimeMetrics(registry);

        metrics.recordClaim("directed", "claimed");
        metrics.recordClaim("run-id-that-must-not-be-a-tag", "unexpected");
        metrics.recordDispatch("scheduled", "created");
        metrics.recordNotificationReceived(RunNotificationPublisher.RUN_CHANNEL, true);
        metrics.recordNotificationReceived("unsupported-channel", false);
        metrics.recordNotificationClaimLatencyNanos(1_000_000);
        metrics.recordPollingScan("periodic", "success");
        metrics.recordPollingLag(Instant.now().minusSeconds(1));
        metrics.recordLeaseRenewal(JobRunStore.LeaseRenewalResult.RENEWED);
        metrics.recordLeaseRecovery("replacement");
        metrics.recordRetry("automatic");
        metrics.recordFencedUpdate("succeeded", JobRunStore.FencedUpdateResult.FENCED);
        metrics.recordCancellation("request", "requested");
        metrics.recordTerminalOutcome(JobRunStatus.SUCCEEDED);
        metrics.updateWorkerCapacity(2, 1);
        metrics.updateListenerConnected(true);
        metrics.updatePollingRunning(true);

        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.CLAIMS)
                .tag("claim_type", "directed").tag("outcome", "claimed").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.CLAIMS)
                .tag("claim_type", "other").tag("outcome", "other").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.DISPATCHES)
                .tag("dispatch_type", "scheduled").tag("outcome", "created").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.NOTIFICATIONS)
                .tag("channel", "run").tag("outcome", "accepted").counter().count());
        assertTrue(registry.get(ManagedRuntimeMetrics.NOTIFICATION_CLAIM_LATENCY).timer().count() > 0);
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.POLLING_SCANS)
                .tag("trigger", "periodic").tag("outcome", "success").counter().count());
        assertTrue(registry.get(ManagedRuntimeMetrics.POLLING_LAG).timer().count() > 0);
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.LEASE_RENEWALS)
                .tag("outcome", "renewed").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.LEASE_RECOVERIES)
                .tag("outcome", "replacement").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.RETRIES)
                .tag("retry_type", "automatic").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.FENCED_UPDATES)
                .tag("operation", "succeeded").tag("outcome", "fenced").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.CANCELLATIONS)
                .tag("operation", "request").tag("outcome", "requested").counter().count());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.TERMINAL_OUTCOMES)
                .tag("status", "succeeded").counter().count());
        assertEquals(2.0, registry.get(ManagedRuntimeMetrics.ACTIVE_WORKER_SLOTS).gauge().value());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.FREE_WORKER_SLOTS).gauge().value());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.LISTENER_CONNECTED).gauge().value());
        assertEquals(1.0, registry.get(ManagedRuntimeMetrics.POLLING_RUNNING).gauge().value());

        assertTrue(registry.getMeters().stream()
                .flatMap(meter -> meter.getId().getTags().stream())
                .noneMatch(tag -> tag.getValue().contains("run-id-that-must-not-be-a-tag")));
        assertFalse(registry.getMeters().stream()
                .anyMatch(meter -> meter.getId().getName().contains("run-id-that-must-not-be-a-tag")));
    }
}
