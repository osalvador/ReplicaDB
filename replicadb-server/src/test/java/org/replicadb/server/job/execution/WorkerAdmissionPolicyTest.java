package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.config.WorkerRuntimeProperties;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerAdmissionPolicyTest {

    @Test
    void appliesJitterOnlyToDirectedAndGenericOpportunities() {
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        AtomicLong now = new AtomicLong();
        WorkerAdmissionPolicy policy = new WorkerAdmissionPolicy(configuration, now::get, () -> 0.5);

        assertEquals(Duration.ofMillis(50), policy.delayFor(AdmissionLane.DIRECTED));
        assertEquals(Duration.ZERO, policy.delayFor(AdmissionLane.FALLBACK));
        assertEquals(Duration.ofMillis(50), policy.delayFor(AdmissionLane.GENERIC));
    }

    @Test
    void cooldownAppliesOnlyToLaterGenericRefills() {
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        AtomicLong now = new AtomicLong();
        WorkerAdmissionPolicy policy = new WorkerAdmissionPolicy(configuration, now::get, () -> 0.5);

        policy.recordSuccessfulClaim();

        assertEquals(Duration.ofMillis(50), policy.delayFor(AdmissionLane.DIRECTED));
        assertEquals(Duration.ZERO, policy.delayFor(AdmissionLane.FALLBACK));
        assertEquals(Duration.ofMillis(300), policy.delayFor(AdmissionLane.GENERIC));

        now.addAndGet(Duration.ofMillis(250).toNanos());
        assertEquals(Duration.ofMillis(50), policy.delayFor(AdmissionLane.GENERIC));
    }

    @Test
    void contentionIsCappedAndDuplicateSignalsDoNotCreateAnotherLane() {
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        AtomicLong now = new AtomicLong();
        WorkerAdmissionPolicy policy = new WorkerAdmissionPolicy(configuration, now::get, () -> 0.0);

        policy.recordDuplicateSignal();
        Duration first = policy.delayFor(AdmissionLane.GENERIC);
        for (int index = 0; index < 20; index++) {
            policy.recordContention();
        }
        Duration capped = policy.delayFor(AdmissionLane.GENERIC);

        assertTrue(first.compareTo(Duration.ZERO) > 0);
        assertTrue(capped.compareTo(Duration.ofSeconds(2)) <= 0);
        assertEquals(Duration.ZERO, policy.delayFor(AdmissionLane.FALLBACK));
    }

    @Test
    void randomJitterStaysWithinConfiguredBounds() {
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        AtomicReference<Double> random = new AtomicReference<>(-1.0);
        WorkerAdmissionPolicy policy = new WorkerAdmissionPolicy(configuration,
                System::nanoTime, random::get);

        assertEquals(Duration.ZERO, policy.delayFor(AdmissionLane.DIRECTED));
        random.set(1.0);
        assertEquals(configuration.getJitterMax(), policy.delayFor(AdmissionLane.DIRECTED));
    }

    @Test
    void successfulWorkResetsContentionBackoff() {
        WorkerRuntimeProperties.Admission configuration = new WorkerRuntimeProperties.Admission();
        AtomicLong now = new AtomicLong();
        WorkerAdmissionPolicy policy = new WorkerAdmissionPolicy(configuration, now::get, () -> 0.0);

        policy.recordContention();
        assertTrue(policy.contentionDelay().compareTo(Duration.ZERO) > 0);
        policy.recordSuccessfulClaim();

        assertEquals(Duration.ZERO, policy.contentionDelay());
    }
}