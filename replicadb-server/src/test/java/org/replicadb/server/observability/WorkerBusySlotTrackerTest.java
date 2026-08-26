package org.replicadb.server.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerBusySlotTrackerTest {

    @Test
    void accountsRawAndNormalizedBusyTimeAcrossSlotTransitions() {
        AtomicLong now = new AtomicLong();
        WorkerBusySlotTracker tracker = new WorkerBusySlotTracker(
                new SimpleMeterRegistry(), "worker-a", 2, now::get);

        tracker.slotAcquired();
        now.addAndGet(Duration.ofSeconds(1).toNanos());
        tracker.slotAcquired();
        now.addAndGet(Duration.ofSeconds(2).toNanos());
        WorkerBusySlotTracker.Snapshot snapshot = tracker.snapshot();

        assertEquals(2, snapshot.activeSlots());
        assertEquals(5.0, snapshot.busySlotSeconds(), 0.0001);
        assertEquals(2.5, snapshot.normalizedBusySlotSeconds(), 0.0001);
    }

    @Test
    void releasedSlotsStopAccumulatingAndCannotBecomeNegative() {
        AtomicLong now = new AtomicLong();
        WorkerBusySlotTracker tracker = new WorkerBusySlotTracker(
                new SimpleMeterRegistry(), "worker-a", 1, now::get);

        tracker.slotReleased();
        assertEquals(0, tracker.snapshot().activeSlots());
        tracker.slotAcquired();
        now.addAndGet(Duration.ofSeconds(2).toNanos());
        tracker.slotReleased();
        now.addAndGet(Duration.ofSeconds(2).toNanos());

        assertEquals(2.0, tracker.snapshot().busySlotSeconds(), 0.0001);
        assertEquals(2.0, tracker.snapshot().normalizedBusySlotSeconds(), 0.0001);
        assertEquals(0, tracker.activeSlots());
    }

    @Test
    void normalizesAndBoundsWorkerIdentityOnMetersAndSnapshot() {
        String longIdentity = " worker/with spaces and a value that is deliberately much longer than sixty-four characters ";
        WorkerBusySlotTracker tracker = new WorkerBusySlotTracker(
                new SimpleMeterRegistry(), longIdentity, 4, System::nanoTime);

        WorkerBusySlotTracker.Snapshot snapshot = tracker.snapshot();

        assertEquals(64, snapshot.workerIdentity().length());
        assertTrue(snapshot.workerIdentity().matches("[A-Za-z0-9._-]+"));
    }

    @Test
    void registersUtilizationGaugesWithBoundedIdentity() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new WorkerBusySlotTracker(registry, "worker/a", 2, System::nanoTime);

        assertEquals(0.0, registry.get(ManagedRuntimeMetrics.BUSY_SLOT_SECONDS)
                .tag("worker_identity", "worker_a").gauge().value());
        assertEquals(0.0, registry.get(ManagedRuntimeMetrics.NORMALIZED_BUSY_SLOT_SECONDS)
                .tag("worker_identity", "worker_a").gauge().value());
    }
}