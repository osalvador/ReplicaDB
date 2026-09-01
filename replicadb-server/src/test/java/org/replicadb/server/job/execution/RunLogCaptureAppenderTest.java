package org.replicadb.server.job.execution;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunLogCaptureAppenderTest {

    private final RunLogCaptureRegistry registry = RunLogCaptureAppender.registry();
    private final RunLogCaptureAppender appender = new RunLogCaptureAppender("test");

    @AfterEach
    void clear() {
        ThreadContext.clearMap();
    }

    @Test
    void capturesOnlyRegisteredRunAndRedactsThrowable() {
        UUID runId = UUID.randomUUID();
        RunLogCaptureRegistry.Capture capture = registry.register(runId);
        ThreadContext.put("replication.runId", runId.toString());
        appender.append(new Log4jLogEvent.Builder()
                .setLevel(Level.ERROR)
                .setMessage(new ParameterizedMessage("password=secret-value"))
                .setThrown(new IllegalStateException("token=secret-token"))
                .setTimeMillis(Instant.parse("2026-09-01T14:30:45.123Z").toEpochMilli())
                .build());

        String content = capture.snapshot().content();
        assertTrue(content.contains("[REDACTED]"));
        assertFalse(content.contains("secret-value"));
        assertFalse(content.contains("secret-token"));
        String expectedTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault())
            .format(Instant.parse("2026-09-01T14:30:45.123Z"));
        assertTrue(content.lines().allMatch(line -> line.startsWith(expectedTimestamp + " ")));
        registry.unregister(capture);
        assertEquals(0, registry.size());
    }

    @Test
    void excludesUnregisteredAndLateEvents() {
        UUID runId = UUID.randomUUID();
        ThreadContext.put("replication.runId", runId.toString());
        appender.append(new Log4jLogEvent.Builder().setMessage(new ParameterizedMessage("ignored")).build());
        RunLogCaptureRegistry.Capture capture = registry.register(runId);
        registry.unregister(capture);
        appender.append(new Log4jLogEvent.Builder().setMessage(new ParameterizedMessage("late")).build());
        assertEquals("", capture.snapshot().content());
    }

    @Test
    void boundsUtf8ContentWithDeterministicMarker() {
        RunLogCaptureRegistry.Capture capture = registry.register(UUID.randomUUID());
        capture.append("first-" + "a".repeat(300_000) + "-last");
        RunLogCaptureRegistry.Snapshot snapshot = capture.snapshot();
        assertTrue(snapshot.truncated());
        assertTrue(snapshot.content().contains(RunLogCaptureRegistry.BoundedText.MARKER));
        assertTrue(snapshot.content().getBytes(StandardCharsets.UTF_8).length <= 256 * 1024);
        registry.unregister(capture);
    }

    @Test
    void redactsSensitiveCorpusFromNestedThrowableOutput() {
        RunLogCaptureRegistry.Capture capture = registry.register(UUID.randomUUID());
        Throwable cause = new IllegalArgumentException("password=jdbc-secret; token=nested-token");
        Throwable failure = new IllegalStateException(
                "postgresql://alice:uri-secret@host/db?password=query-secret "
                        + "${env:MASTER_KEY} -----BEGIN PRIVATE KEY-----", cause);
        capture.append(failure.toString() + "\n" + cause + "\n" + "-----BEGIN CERTIFICATE-----");

        String content = capture.snapshot().content();
        assertFalse(content.contains("uri-secret"));
        assertFalse(content.contains("query-secret"));
        assertFalse(content.contains("nested-token"));
        assertFalse(content.contains("MASTER_KEY"));
        assertFalse(content.contains("PRIVATE KEY"), content);
        assertFalse(content.contains("CERTIFICATE"));
        registry.unregister(capture);
    }
}
