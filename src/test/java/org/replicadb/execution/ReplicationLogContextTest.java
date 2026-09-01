package org.replicadb.execution;

import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ReplicationLogContextTest {

    @AfterEach
    void clearContext() {
        ThreadContext.clearMap();
    }

    @Test
    void restoresNestedAndStandaloneContext() {
        ThreadContext.put("request", "outer");
        try (ReplicationLogContext.Scope outer = ReplicationLogContext.bind(new ReplicationExecutionContext())) {
            String outerRunId = ThreadContext.get(ReplicationLogContext.RUN_ID_KEY);
            try (ReplicationLogContext.Scope inner = ReplicationLogContext.bind(new ReplicationExecutionContext())) {
                assertFalse(outerRunId.equals(ThreadContext.get(ReplicationLogContext.RUN_ID_KEY)));
            }
            assertEquals(outerRunId, ThreadContext.get(ReplicationLogContext.RUN_ID_KEY));
            assertEquals("outer", ThreadContext.get("request"));
        }
        assertEquals(Map.of("request", "outer"), ThreadContext.getImmutableContext());

        try (ReplicationLogContext.Scope ignored = ReplicationLogContext.bind(null)) {
            assertFalse(ThreadContext.containsKey(ReplicationLogContext.RUN_ID_KEY));
        }
    }

    @Test
    void explicitlyPropagatesAndClearsContextOnExecutorThread() throws Exception {
        ReplicationExecutionContext context = new ReplicationExecutionContext();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Map<String, String>> first = executor.submit(() -> {
                try (ReplicationLogContext.Scope ignored = ReplicationLogContext.install(
                        Map.of("request", "parent"), context)) {
                    return ThreadContext.getImmutableContext();
                }
            });
            assertEquals(context.getRunId(), first.get().get(ReplicationLogContext.RUN_ID_KEY));
            Future<Map<String, String>> second = executor.submit(ThreadContext::getImmutableContext);
            assertEquals(Map.of(), second.get());
        } finally {
            executor.shutdownNow();
        }
    }
}
