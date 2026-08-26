package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerAdmissionQueueTest {

    @Test
    void coalescesDistinctDirectedSignalsAndPreservesTheFirstTimestamp() {
        WorkerAdmissionQueue queue = new WorkerAdmissionQueue(2);
        UUID runId = UUID.randomUUID();

        assertEquals(WorkerAdmissionQueue.OfferResult.ADDED, queue.offerDirected(runId, 10));
        assertEquals(WorkerAdmissionQueue.OfferResult.COALESCED, queue.offerDirected(runId, 20));

        WorkerAdmissionQueue.DirectedSignal signal = queue.pollDirected().orElseThrow();
        assertEquals(10, signal.receivedNanos());
        assertEquals(WorkerAdmissionQueue.SignalState.SCHEDULED, signal.state());
        assertEquals(1, queue.directedSize());
        assertTrue(queue.completeDirected(runId));
        assertEquals(0, queue.directedSize());
    }

    @Test
    void boundsDirectedSignalsAndAllowsScheduledSignalsToBeRequeued() {
        WorkerAdmissionQueue queue = new WorkerAdmissionQueue(1);
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        assertEquals(WorkerAdmissionQueue.OfferResult.ADDED, queue.offerDirected(first, 1));
        assertEquals(WorkerAdmissionQueue.OfferResult.DROPPED, queue.offerDirected(second, 2));
        assertEquals(first, queue.pollDirected().orElseThrow().runId());
        assertTrue(queue.requeueDirected(first));
        assertEquals(1, queue.queuedDirectedSize());
        assertEquals(first, queue.pollDirected().orElseThrow().runId());
        assertFalse(queue.requeueDirected(second));
    }

    @Test
    void coalescesGenericRefillRequestsAndRestoresConsumedRequests() {
        WorkerAdmissionQueue queue = new WorkerAdmissionQueue(1);

        assertTrue(queue.requestGenericRefill("startup"));
        assertFalse(queue.requestGenericRefill("periodic"));
        assertEquals(Optional.of("startup"), queue.pollGenericRefill());
        assertFalse(queue.hasGenericRefill());
        queue.restoreGenericRefill("reconnect");
        assertEquals(Optional.of("reconnect"), queue.pollGenericRefill());
    }
}