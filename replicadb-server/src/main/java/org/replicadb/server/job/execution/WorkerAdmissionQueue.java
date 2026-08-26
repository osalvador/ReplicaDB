package org.replicadb.server.job.execution;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class WorkerAdmissionQueue {

    private final int directedCapacity;
    private final Map<UUID, DirectedSignal> directedSignals = new ConcurrentHashMap<>();
    private final ArrayDeque<UUID> directedOrder = new ArrayDeque<>();
    private boolean genericRefillRequested;
    private String genericRefillTrigger;

    public WorkerAdmissionQueue(int directedCapacity) {
        if (directedCapacity < 1) {
            throw new IllegalArgumentException("directedCapacity must be positive");
        }
        this.directedCapacity = directedCapacity;
    }

    public synchronized OfferResult offerDirected(UUID runId, long receivedNanos) {
        Objects.requireNonNull(runId, "runId must not be null");
        DirectedSignal signal = new DirectedSignal(runId, receivedNanos, SignalState.QUEUED);
        if (directedSignals.putIfAbsent(runId, signal) != null) {
            return OfferResult.COALESCED;
        }
        if (directedSignals.size() > directedCapacity) {
            directedSignals.remove(runId, signal);
            return OfferResult.DROPPED;
        }
        directedOrder.addLast(runId);
        return OfferResult.ADDED;
    }

    public synchronized Optional<DirectedSignal> pollDirected() {
        while (!directedOrder.isEmpty()) {
            UUID runId = directedOrder.removeFirst();
            DirectedSignal signal = directedSignals.get(runId);
            if (signal != null && signal.state() == SignalState.QUEUED) {
                DirectedSignal scheduled = new DirectedSignal(signal.runId(), signal.receivedNanos(),
                        SignalState.SCHEDULED);
                directedSignals.put(runId, scheduled);
                return Optional.of(scheduled);
            }
        }
        return Optional.empty();
    }

    public synchronized boolean requeueDirected(UUID runId) {
        DirectedSignal signal = directedSignals.get(runId);
        if (signal == null || signal.state() != SignalState.SCHEDULED) {
            return false;
        }
        directedSignals.put(runId, new DirectedSignal(signal.runId(), signal.receivedNanos(), SignalState.QUEUED));
        directedOrder.addLast(runId);
        return true;
    }

    public synchronized boolean completeDirected(UUID runId) {
        return directedSignals.remove(runId) != null;
    }

    public synchronized boolean requestGenericRefill(String trigger) {
        if (genericRefillRequested) {
            return false;
        }
        genericRefillRequested = true;
        genericRefillTrigger = trigger == null || trigger.isBlank() ? "other" : trigger;
        return true;
    }

    public synchronized Optional<String> pollGenericRefill() {
        if (!genericRefillRequested) {
            return Optional.empty();
        }
        genericRefillRequested = false;
        String trigger = genericRefillTrigger;
        genericRefillTrigger = null;
        return Optional.ofNullable(trigger);
    }

    public synchronized boolean hasGenericRefill() {
        return genericRefillRequested;
    }

    public synchronized void restoreGenericRefill(String trigger) {
        if (!genericRefillRequested) {
            genericRefillRequested = true;
            genericRefillTrigger = trigger == null || trigger.isBlank() ? "other" : trigger;
        }
    }

    public synchronized int directedSize() {
        return directedSignals.size();
    }

    public synchronized int queuedDirectedSize() {
        return (int) directedSignals.values().stream()
                .filter(signal -> signal.state() == SignalState.QUEUED)
                .count();
    }

    public synchronized void clear() {
        directedSignals.clear();
        directedOrder.clear();
        genericRefillRequested = false;
        genericRefillTrigger = null;
    }

    public enum OfferResult {
        ADDED,
        COALESCED,
        DROPPED
    }

    enum SignalState {
        QUEUED,
        SCHEDULED
    }

    public record DirectedSignal(UUID runId, long receivedNanos, SignalState state) {
    }
}