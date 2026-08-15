package org.replicadb;

import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReplicaDBTaskSummaryTest {

    @Test
    void summarizesEmptyResults() {
        ReplicaDB.ReplicaTaskResultsSummary summary = ReplicaDB.summarize(List.of());

        assertEquals(0, summary.totalRowsProcessed());
        assertEquals(0, summary.maxDurationMillis());
        assertEquals(0, summary.taskCount());
    }

    @Test
    void sumsRowsAndSelectsLongestTask() {
        ReplicaDB.ReplicaTaskResultsSummary summary = ReplicaDB.summarize(List.of(
                new ReplicaTaskResult(0, 10, 100, 150, null),
                new ReplicaTaskResult(1, 20, 200, 320, null),
                new ReplicaTaskResult(2, 7, 300, 301, null)));

        assertEquals(37, summary.totalRowsProcessed());
        assertEquals(120, summary.maxDurationMillis());
        assertEquals(3, summary.taskCount());
    }

    @Test
    void summarizesSingleResult() {
        ReplicaDB.ReplicaTaskResultsSummary summary = ReplicaDB.summarize(List.of(
                new ReplicaTaskResult(0, 5, 100, 100, null)));

        assertEquals(5, summary.totalRowsProcessed());
        assertEquals(0, summary.maxDurationMillis());
        assertEquals(1, summary.taskCount());
    }

    @Test
    void reducesWatermarkCandidatesToMaximum() {
        ReplicaDB.ReplicaTaskResultsSummary summary = ReplicaDB.summarize(List.of(
                new ReplicaTaskResult(0, 5, 100, 150, "9"),
                new ReplicaTaskResult(1, 5, 100, 150, "10"),
                new ReplicaTaskResult(2, 5, 100, 150, null)), Types.INTEGER);

        assertEquals("10", summary.watermarkCandidate());
    }

    @Test
    void summarizeSingleArgOverloadKeepsNullWatermark() {
        ReplicaDB.ReplicaTaskResultsSummary summary = ReplicaDB.summarize(List.of(
                new ReplicaTaskResult(0, 10, 100, 150, null),
                new ReplicaTaskResult(1, 20, 200, 320, null)));

        assertNull(summary.watermarkCandidate());
    }
}
