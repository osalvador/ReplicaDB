package org.replicadb.server.job.domain;

import org.replicadb.cli.ReplicationMode;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public final class JobDefinitionTestFixtures {

    private JobDefinitionTestFixtures() {
    }

    public static Builder aJobDefinition() {
        return new Builder();
    }

    public static final class Builder {

        private UUID id;
        private String name = "job-" + UUID.randomUUID();
        private String sourceConnect = "jdbc:source";
        private String sourceUser;
        private String sourcePassword;
        private String sourceTable = "source_table";
        private String sourceColumns;
        private String sourceWhere;
        private String sourceQuery;
        private AzureAuthentication sourceAuthentication;
        private Map<String, String> sourceConnectionParams = Map.of();
        private String sinkConnect = "jdbc:sink";
        private String sinkUser;
        private String sinkPassword;
        private String sinkTable = "sink_table";
        private String sinkColumns;
        private AzureAuthentication sinkAuthentication;
        private Map<String, String> sinkConnectionParams = Map.of();
        private StagingOptions staging;
        private boolean sinkDisableEscape;
        private boolean sinkDisableTruncate;
        private ReplicationMode mode = ReplicationMode.COMPLETE;
        private int jobs = 1;
        private String incrementalWatermarkColumn;
        private String initialWatermarkValue;
        private Instant createdAt;
        private Instant updatedAt;
        private int fetchSize = 100;
        private int bandwidthThrottling;
        private boolean verbose;

        public Builder withId(UUID value) {
            id = value;
            return this;
        }

        public Builder withName(String value) {
            name = value;
            return this;
        }

        public Builder withSourceConnect(String value) {
            sourceConnect = value;
            return this;
        }

        public Builder withSourceUser(String value) {
            sourceUser = value;
            return this;
        }

        public Builder withSourcePassword(String value) {
            sourcePassword = value;
            return this;
        }

        public Builder withSourceTable(String value) {
            sourceTable = value;
            return this;
        }

        public Builder withSourceColumns(String value) {
            sourceColumns = value;
            return this;
        }

        public Builder withSourceWhere(String value) {
            sourceWhere = value;
            return this;
        }

        public Builder withSourceQuery(String value) {
            sourceQuery = value;
            return this;
        }

        public Builder withSourceAuthentication(AzureAuthentication value) {
            sourceAuthentication = value;
            return this;
        }

        public Builder withSourceConnectionParams(Map<String, String> value) {
            sourceConnectionParams = value;
            return this;
        }

        public Builder withSinkConnect(String value) {
            sinkConnect = value;
            return this;
        }

        public Builder withSinkUser(String value) {
            sinkUser = value;
            return this;
        }

        public Builder withSinkPassword(String value) {
            sinkPassword = value;
            return this;
        }

        public Builder withSinkTable(String value) {
            sinkTable = value;
            return this;
        }

        public Builder withSinkColumns(String value) {
            sinkColumns = value;
            return this;
        }

        public Builder withSinkAuthentication(AzureAuthentication value) {
            sinkAuthentication = value;
            return this;
        }

        public Builder withSinkConnectionParams(Map<String, String> value) {
            sinkConnectionParams = value;
            return this;
        }

        public Builder withSinkStaging(String schema, String table) {
            staging = schema == null && table == null ? null : new StagingOptions(schema, table);
            return this;
        }

        public Builder withSinkDisableEscape(boolean value) {
            sinkDisableEscape = value;
            return this;
        }

        public Builder withSinkDisableTruncate(boolean value) {
            sinkDisableTruncate = value;
            return this;
        }

        public Builder withMode(ReplicationMode value) {
            mode = value;
            return this;
        }

        public Builder withJobs(int value) {
            jobs = value;
            return this;
        }

        public Builder withIncrementalWatermarkColumn(String value) {
            incrementalWatermarkColumn = value;
            return this;
        }

        public Builder withInitialWatermarkValue(String value) {
            initialWatermarkValue = value;
            return this;
        }

        public Builder withCreatedAt(Instant value) {
            createdAt = value;
            return this;
        }

        public Builder withUpdatedAt(Instant value) {
            updatedAt = value;
            return this;
        }

        public Builder withFetchSize(int value) {
            fetchSize = value;
            return this;
        }

        public Builder withBandwidthThrottling(int value) {
            bandwidthThrottling = value;
            return this;
        }

        public Builder withVerbose(boolean value) {
            verbose = value;
            return this;
        }

        public JobDefinition build() {
            return new JobDefinition(
                    id, name,
                    new SourceEndpoint(new ConnectionCredentials(sourceConnect, sourceUser, sourcePassword,
                            sourceAuthentication, sourceConnectionParams), sourceTable, sourceColumns, sourceWhere,
                            sourceQuery),
                    new SinkEndpoint(new ConnectionCredentials(sinkConnect, sinkUser, sinkPassword,
                            sinkAuthentication, sinkConnectionParams), sinkTable, sinkColumns, staging,
                            sinkDisableEscape, sinkDisableTruncate),
                    mode, jobs, incrementalWatermarkColumn, initialWatermarkValue, createdAt, updatedAt,
                    fetchSize, bandwidthThrottling, verbose);
        }
    }
}
