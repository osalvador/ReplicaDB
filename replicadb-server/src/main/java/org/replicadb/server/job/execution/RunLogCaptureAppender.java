package org.replicadb.server.job.execution;

import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.replicadb.execution.ReplicationLogContext;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Plugin(name = "RunLogCapture", category = "Core", printObject = true)
public final class RunLogCaptureAppender extends AbstractAppender {

    private static final RunLogCaptureRegistry REGISTRY = new RunLogCaptureRegistry();
        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    public RunLogCaptureAppender(String name) {
        super(name, null, null, true, null);
    }

    @PluginFactory
    public static RunLogCaptureAppender createAppender(String name) {
        return new RunLogCaptureAppender(name == null ? "RunLogCapture" : name);
    }

    public static RunLogCaptureRegistry registry() {
        return REGISTRY;
    }

    @Override
    public void append(LogEvent event) {
        try {
            RunLogCaptureRegistry.Capture capture = REGISTRY.find(ThreadContext.get(ReplicationLogContext.RUN_ID_KEY));
            if (capture == null) {
                return;
            }
            StringBuilder rendered = new StringBuilder(event.getMessage().getFormattedMessage());
            if (event.getThrown() != null) {
                StringWriter writer = new StringWriter();
                event.getThrown().printStackTrace(new PrintWriter(writer));
                rendered.append('\n').append(writer);
            }
            capture.append(timestampLines(event.getTimeMillis(), rendered.toString()));
        } catch (RuntimeException ignored) {
        }
    }

    private static String timestampLines(long timeMillis, String value) {
        String timestamp = TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(timeMillis));
        String timestamped = java.util.Arrays.stream(value.split("\\R"))
                .map(line -> timestamp + " " + line)
                .collect(java.util.stream.Collectors.joining("\n"));
        return timestamped + "\n";
    }

}
