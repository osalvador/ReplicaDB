package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.api.RunLogResponse;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunLogTest {

    @Test
    void validatesBoundedContentAndRequiredFields() {
        UUID runId = UUID.randomUUID();
        Instant now = Instant.now();
        RunLog log = new RunLog(runId, "", false, 0, 1, now, now);

        assertEquals(runId, log.runId());
        assertEquals("", log.content());
        assertThrows(NullPointerException.class, () -> new RunLog(null, "", false, 0, 1, now, now));
        assertThrows(IllegalArgumentException.class,
                () -> new RunLog(runId, "x", false, 0, 1, now, now));
        assertThrows(IllegalArgumentException.class,
                () -> new RunLog(runId, "x".repeat(RunLog.MAX_BYTES + 1), false,
                        RunLog.MAX_BYTES, 1, now, now));
    }

    @Test
    void mapsMetadataAndProvidesSafeEmptyResponse() {
        UUID runId = UUID.randomUUID();
        Instant now = Instant.now();
        RunLog log = new RunLog(runId, "line\nstack", true, 300_000, 1, now, now);
        RunLogResponse response = RunLogResponse.from(log);

        assertEquals(log.content(), response.content());
        assertEquals(log.capturedSize(), response.capturedSize());
        assertEquals(runId, RunLogResponse.empty(runId).runId());
        assertEquals("", RunLogResponse.empty(runId).content());
    }
}
