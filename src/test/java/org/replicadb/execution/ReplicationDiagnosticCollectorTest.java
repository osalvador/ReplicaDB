package org.replicadb.execution;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationDiagnosticCollectorTest {

    @Test
    void validatesEventsAndNormalizesOptionalText() {
        ReplicationDiagnosticEvent event = new ReplicationDiagnosticEvent(Instant.EPOCH,
                ReplicationDiagnosticEvent.Stage.SOURCE_READ, ReplicationDiagnosticEvent.Category.READ,
                ReplicationDiagnosticEvent.Severity.ERROR, " ", "reader", "failed", " ", " ");

        assertNull(event.taskId());
        assertNull(event.throwableSummary());
        assertThrows(IllegalArgumentException.class, () -> new ReplicationDiagnosticEvent(
                Instant.EPOCH, ReplicationDiagnosticEvent.Stage.CLEANUP,
                ReplicationDiagnosticEvent.Category.CLEANUP, ReplicationDiagnosticEvent.Severity.ERROR,
                null, " ", "message", null, null));
    }

    @Test
    void capturesStacktraceOrderingAndImmutableSnapshot() {
        ReplicationDiagnosticCollector.Bounded collector = new ReplicationDiagnosticCollector.Bounded();
        collector.record(ReplicationDiagnosticEvent.Stage.SOURCE_CONNECTION,
                ReplicationDiagnosticEvent.Category.CONNECTION, ReplicationDiagnosticEvent.Severity.ERROR,
                "1", "source", "connection failed", new IllegalStateException("boom"));
        collector.record(ReplicationDiagnosticEvent.Stage.CLEANUP,
                ReplicationDiagnosticEvent.Category.CLEANUP, ReplicationDiagnosticEvent.Severity.INFO,
                null, "cleanup", "done", null);

        ReplicationDiagnosticCollector.Snapshot snapshot = collector.snapshot();
        assertEquals(2, snapshot.events().size());
        assertTrue(snapshot.content().indexOf("connection failed") < snapshot.content().indexOf("done"));
        assertTrue(snapshot.content().contains("IllegalStateException: boom"));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.events().clear());
    }

    @Test
    void boundsUtf8ContentAndReportsOriginalBytes() {
        ReplicationDiagnosticCollector.Bounded collector = new ReplicationDiagnosticCollector.Bounded();
        String message = "a".repeat(300_000);
        collector.record(new ReplicationDiagnosticEvent(Instant.EPOCH,
                ReplicationDiagnosticEvent.Stage.SOURCE_READ, ReplicationDiagnosticEvent.Category.READ,
                ReplicationDiagnosticEvent.Severity.DEBUG, null, "reader", message, null, null));

        ReplicationDiagnosticCollector.Snapshot snapshot = collector.snapshot();
        assertTrue(snapshot.truncated());
        assertTrue(snapshot.content().getBytes(StandardCharsets.UTF_8).length <= ReplicationDiagnosticCollector.MAX_BYTES);
        assertTrue(snapshot.capturedBytes() > ReplicationDiagnosticCollector.MAX_BYTES);
        assertTrue(snapshot.content().contains(ReplicationDiagnosticCollector.TRUNCATION_MARKER));
    }

    @Test
    void supportsConcurrentWritesWithoutExceedingBound() throws Exception {
        ReplicationDiagnosticCollector.Bounded collector = new ReplicationDiagnosticCollector.Bounded();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int index = 0; index < 100; index++) {
            int task = index;
            executor.submit(() -> collector.record(ReplicationDiagnosticEvent.Stage.SINK_WRITE,
                    ReplicationDiagnosticEvent.Category.WRITE, ReplicationDiagnosticEvent.Severity.WARN,
                    Integer.toString(task), "writer", "message-" + task, null));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(100, collector.snapshot().events().size());
        assertTrue(collector.snapshot().capturedBytes() > 0);
    }
}
