package org.replicadb.execution;

import org.apache.logging.log4j.ThreadContext;

import java.util.Map;

public final class ReplicationLogContext {

    public static final String RUN_ID_KEY = "replication.runId";

    private ReplicationLogContext() {
    }

    public static Map<String, String> capture() {
        return ThreadContext.getImmutableContext();
    }

    public static Scope bind(ReplicationExecutionContext context) {
        return bindRunId(context == null ? null : context.getRunId());
    }

    public static Scope bindRunId(String runId) {
        Map<String, String> previous = capture();
        if (runId == null) {
            ThreadContext.remove(RUN_ID_KEY);
        } else {
            ThreadContext.put(RUN_ID_KEY, runId);
        }
        return new Scope(previous);
    }

    public static Scope install(Map<String, String> parentContext, ReplicationExecutionContext context) {
        Map<String, String> previous = capture();
        ThreadContext.clearMap();
        if (parentContext != null) {
            ThreadContext.putAll(parentContext);
        }
        if (context != null) {
            ThreadContext.put(RUN_ID_KEY, context.getRunId());
        }
        return new Scope(previous);
    }

    public static final class Scope implements AutoCloseable {
        private final Map<String, String> previous;
        private boolean closed;

        private Scope(Map<String, String> previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (!closed) {
                ThreadContext.clearMap();
                ThreadContext.putAll(previous);
                closed = true;
            }
        }
    }
}
