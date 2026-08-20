package org.replicadb.server.job.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ConnectionCredentials;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.RetryPolicy;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.domain.StagingOptions;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Locale;

public final class JobDefinitionRowMapper implements RowMapper<JobDefinition> {

    private final ObjectMapper objectMapper;

    public JobDefinitionRowMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public JobDefinition mapRow(ResultSet resultSet, int rowNum) throws SQLException {
        return new JobDefinition(
                resultSet.getObject("id", java.util.UUID.class),
                resultSet.getString("name"),
                new SourceEndpoint(
                        new ConnectionCredentials(
                                resultSet.getString("source_connect"),
                                resultSet.getString("source_user"),
                                resultSet.getString("source_password"),
                                new AzureAuthentication(
                                        resultSet.getString("source_auth_mode"),
                                        resultSet.getString("source_auth_principal_id"),
                                        resultSet.getString("source_auth_login_hint"),
                                        resultSet.getString("source_auth_client_certificate"),
                                        resultSet.getString("source_auth_client_key")),
                                deserializeConnectionParams(resultSet.getString("source_connection_params"))),
                        resultSet.getString("source_table"),
                        resultSet.getString("source_columns"),
                        resultSet.getString("source_where"),
                        resultSet.getString("source_query")),
                new SinkEndpoint(
                        new ConnectionCredentials(
                                resultSet.getString("sink_connect"),
                                resultSet.getString("sink_user"),
                                resultSet.getString("sink_password"),
                                new AzureAuthentication(
                                        resultSet.getString("sink_auth_mode"),
                                        resultSet.getString("sink_auth_principal_id"),
                                        resultSet.getString("sink_auth_login_hint"),
                                        resultSet.getString("sink_auth_client_certificate"),
                                        resultSet.getString("sink_auth_client_key")),
                                deserializeConnectionParams(resultSet.getString("sink_connection_params"))),
                        resultSet.getString("sink_table"),
                        resultSet.getString("sink_columns"),
                        stagingOptions(resultSet.getString("sink_staging_schema"),
                                resultSet.getString("sink_staging_table")),
                        resultSet.getBoolean("sink_disable_escape"),
                        resultSet.getBoolean("sink_disable_truncate")),
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

    private Map<String, String> deserializeConnectionParams(String connectionParams) {
        if (connectionParams == null || connectionParams.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, String> result = objectMapper.readValue(
                    connectionParams, new TypeReference<Map<String, String>>() { });
            return result == null ? Map.of() : result;
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not deserialize connection parameters", exception);
        }
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
