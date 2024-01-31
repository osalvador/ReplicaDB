package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

public class Sentry {
    private static final Logger LOG = LogManager.getLogger(Sentry.class);

    /**
     * Init the Sentry service for error tracking and performance monitoring.
     *
     * @param options
     */
    public static void SentryInit(ToolOptions options) {

        String sentryDsn = options.getSentryDsn() != null ? options.getSentryDsn() : "";

        // Init Sentry
        io.sentry.Sentry.init(sentryOptions -> {
            sentryOptions.setEnableExternalConfiguration(true);
            sentryOptions.setTracesSampleRate(1.0);
            sentryOptions.setRelease("replicadb@" + options.getVersion());
            sentryOptions.setDsn(sentryDsn);
        });

        if (io.sentry.Sentry.isEnabled()) LOG.info("Sentry enabled");

        // Sentry Context
        io.sentry.Sentry.configureScope(scope -> {
            if (options.getSourceTable() != null) scope.setContexts("sourceTable", options.getSourceTable());
            if (options.getSourceColumns() != null) scope.setContexts("sourceColumns", options.getSourceColumns());
            if (options.getSourceWhere() != null) scope.setContexts("sourceWhere", options.getSourceWhere());
            if (options.getSourceQuery() != null) scope.setContexts("sourceQuery", options.getSourceQuery());
            if (options.getSinkTable() != null) scope.setContexts("sinkTable", options.getSinkTable());
            if (options.getSinkStagingTable() != null)
                scope.setContexts("sinkStagingTable", options.getSinkStagingTable());
            if (options.getSinkStagingSchema() != null)
                scope.setContexts("sinkStagingSchema", options.getSinkStagingSchema());
            if (options.getSinkStagingTableAlias() != null)
                scope.setContexts("sinkStagingTableAlias", options.getSinkStagingTableAlias());
            if (options.getSinkColumns() != null) scope.setContexts("sinkColumns", options.getSinkColumns());
            if (options.getQuotedIdentifiers() != null)
                scope.setContexts("quotedIdentifiers", options.getQuotedIdentifiers());
            scope.setContexts("bandwidthThrottling", options.getBandwidthThrottling());
            if (options.getSourceConnectionParams() != null)
                scope.setContexts("sourceConnectionParams", options.getSourceConnectionParams());
            if (options.getSinkConnectionParams() != null)
                scope.setContexts("sinkConnectionParams", options.getSinkConnectionParams());
            // Tags
            scope.setTag("mode", options.getMode());
            scope.setTag("jobs", String.valueOf(options.getJobs()));
            scope.setTag("fetchSize", String.valueOf(options.getFetchSize()));
            scope.setTag("source.connect", options.getSourceConnect());
            scope.setTag("sink.connect", options.getSinkConnect());
            if (options.getVersion() != null) scope.setTag("release.version", options.getVersion());
            if (options.getSinkFileFormat() != null) scope.setTag("sink.file_format", options.getSinkFileFormat());
            if (options.getSourceFileFormat() != null)
                scope.setTag("source.file_format", options.getSourceFileFormat());
        });

    }

}
