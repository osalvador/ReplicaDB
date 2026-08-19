package org.replicadb.server.job.domain;

import org.replicadb.cli.ReplicationMode;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record JobDefinition(
        UUID id,
        String name,
        SourceEndpoint source,
        SinkEndpoint sink,
        ReplicationMode mode,
        int jobs,
        String incrementalWatermarkColumn,
        String initialWatermarkValue,
        Instant createdAt,
        Instant updatedAt,
        int fetchSize,
        int bandwidthThrottling,
        boolean verbose) {

    public JobDefinition {
        requireNonBlank("name", name);
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(sink, "sink must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        if (jobs < 1) {
            throw new IllegalArgumentException("jobs must be at least 1");
        }
        if (fetchSize < 1) {
            throw new IllegalArgumentException("fetchSize must be at least 1");
        }
        if (bandwidthThrottling < 0) {
            throw new IllegalArgumentException("bandwidthThrottling must not be negative");
        }
        if (incrementalWatermarkColumn != null && mode != ReplicationMode.INCREMENTAL) {
            throw new IllegalArgumentException("incrementalWatermarkColumn requires incremental mode");
        }
    }

    public JobDefinition(UUID id, String name,
                         String sourceConnect, String sourceUser, String sourcePassword,
                         String sourceTable, String sourceWhere,
                         String sinkConnect, String sinkUser, String sinkPassword,
                         String sinkTable, ReplicationMode mode, int jobs,
                         String incrementalWatermarkColumn, String initialWatermarkValue,
                         Instant createdAt, Instant updatedAt) {
        this(id, name,
                new SourceEndpoint(new ConnectionCredentials(sourceConnect, sourceUser, sourcePassword, null, null),
                        sourceTable, null, sourceWhere, null),
                new SinkEndpoint(new ConnectionCredentials(sinkConnect, sinkUser, sinkPassword, null, null),
                        sinkTable, null, null, false, false),
                mode, jobs, incrementalWatermarkColumn, initialWatermarkValue,
                createdAt, updatedAt, 100, 0, false);
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    public String sourceConnect() {
        return source.connection().connect();
    }

    public String sourceUser() {
        return source.connection().user();
    }

    public String sourcePassword() {
        return source.connection().password();
    }

    public String sourceTable() {
        return source.table();
    }

    public String sourceWhere() {
        return source.where();
    }

    public String sourceColumns() {
        return source.columns();
    }

    public String sourceQuery() {
        return source.query();
    }

    public AzureAuthentication sourceAuthentication() {
        return source.connection().authentication();
    }

    public java.util.Map<String, String> sourceConnectionParams() {
        return source.connection().connectionParams();
    }

    public String sinkConnect() {
        return sink.connection().connect();
    }

    public String sinkUser() {
        return sink.connection().user();
    }

    public String sinkPassword() {
        return sink.connection().password();
    }

    public String sinkTable() {
        return sink.table();
    }

    public String sinkColumns() {
        return sink.columns();
    }

    public AzureAuthentication sinkAuthentication() {
        return sink.connection().authentication();
    }

    public java.util.Map<String, String> sinkConnectionParams() {
        return sink.connection().connectionParams();
    }

    public String sinkStagingSchema() {
        return sink.staging() == null ? null : sink.staging().schema();
    }

    public String sinkStagingTable() {
        return sink.staging() == null ? null : sink.staging().table();
    }

    public boolean sinkDisableEscape() {
        return sink.disableEscape();
    }

    public boolean sinkDisableTruncate() {
        return sink.disableTruncate();
    }
}
