package org.replicadb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplicaTaskResultTest {

    @Test
    void calculatesTaskDuration() {
        ReplicaTaskResult result = new ReplicaTaskResult(1, 42, 1000, 1350, null);

        assertEquals(350, result.durationMillis());
    }

    @Test
    void rejectsNegativeRowCount() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicaTaskResult(1, -1, 1000, 1350, null));
    }

    @Test
    void rejectsFinishedTimeBeforeStartTime() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicaTaskResult(1, 42, 1350, 1000, null));
    }
}
