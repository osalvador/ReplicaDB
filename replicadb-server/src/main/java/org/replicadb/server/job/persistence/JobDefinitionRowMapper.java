package org.replicadb.server.job.persistence;

import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.domain.StagingOptions;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

public final class JobDefinitionRowMapper implements RowMapper<JobDefinition> {

    @Override
    public JobDefinition mapRow(ResultSet resultSet, int rowNum) throws SQLException {
        return new JobDefinition(
                resultSet.getObject("id", java.util.UUID.class),
                resultSet.getString("name"),
            new SourceEndpoint(
                resultSet.getObject("source_datasource_id", java.util.UUID.class),
                        resultSet.getString("source_table"),
                        resultSet.getString("source_columns"),
                        resultSet.getString("source_where"),
                        resultSet.getString("source_query")),
                new SinkEndpoint(
                resultSet.getObject("sink_datasource_id", java.util.UUID.class),
                        resultSet.getString("sink_table"),
                        resultSet.getString("sink_columns"),
                        stagingOptions(resultSet.getString("sink_staging_schema"),
                                resultSet.getString("sink_staging_table")),
                        resultSet.getBoolean("sink_disable_escape"),
                        resultSet.getBoolean("sink_disable_truncate")),
                resultSet.getBoolean("source_datasource_use_enabled"),
                resultSet.getBoolean("sink_datasource_use_enabled"),
                parseMode(resultSet.getString("mode")),
                resultSet.getInt("jobs"),
                resultSet.getString("incremental_watermark_column"),
                resultSet.getString("initial_watermark_value"),
                resultSet.getTimestamp("created_at").toInstant(),
                resultSet.getTimestamp("updated_at").toInstant(),
                integerOrDefault(resultSet, "fetch_size", 100),
                integerOrDefault(resultSet, "bandwidth_throttling", 0),
                resultSet.getBoolean("verbose"),
                new RetryPolicy(resultSet.getInt("max_attempts"),
                        resultSet.getLong("retry_backoff_seconds"),
                        resultSet.getBoolean("automatic_retry_enabled")));
    }

    private static StagingOptions stagingOptions(String schema, String table) {
        return schema == null && table == null ? null : new StagingOptions(schema, table);
    }

    private static int integerOrDefault(ResultSet resultSet, String column, int defaultValue) throws SQLException {
        int value = resultSet.getInt(column);
        return resultSet.wasNull() ? defaultValue : value;
    }

    private static ReplicationMode parseMode(String modeText) {
        for (ReplicationMode mode : ReplicationMode.values()) {
            if (mode.getModeText().equals(modeText.toLowerCase(Locale.ROOT))) {
                return mode;
            }
        }
        throw new IllegalStateException("Unknown replication mode: " + modeText);
    }
}
