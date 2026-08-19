package org.replicadb.server.job.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ConnectionCredentials;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.SinkEndpoint;
import org.replicadb.server.job.domain.SourceEndpoint;
import org.replicadb.server.job.domain.StagingOptions;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.sql.Types;

@Repository
public class JobDefinitionRepository {

    private static final String INSERT_SQL = """
            INSERT INTO job_definition (
                id, name, source_connect, source_user, source_password, source_table, source_where,
                source_auth_mode, source_auth_principal_id, source_auth_login_hint,
                source_auth_client_certificate, source_auth_client_key, source_connection_params,
                source_columns, source_query,
                sink_connect, sink_user, sink_password, sink_table,
                sink_auth_mode, sink_auth_principal_id, sink_auth_login_hint,
                sink_auth_client_certificate, sink_auth_client_key, sink_connection_params,
                sink_columns, sink_staging_schema, sink_staging_table,
                sink_disable_escape, sink_disable_truncate, mode, jobs,
                incremental_watermark_column, initial_watermark_value, created_at, updated_at,
                fetch_size, bandwidth_throttling, "verbose"
            ) VALUES (
                :id, :name, :sourceConnect, :sourceUser, :sourcePassword, :sourceTable, :sourceWhere,
                :sourceAuthMode, :sourceAuthPrincipalId, :sourceAuthLoginHint,
                :sourceAuthClientCertificate, :sourceAuthClientKey, CAST(:sourceConnectionParams AS jsonb),
                :sourceColumns, :sourceQuery,
                :sinkConnect, :sinkUser, :sinkPassword, :sinkTable,
                :sinkAuthMode, :sinkAuthPrincipalId, :sinkAuthLoginHint,
                :sinkAuthClientCertificate, :sinkAuthClientKey, CAST(:sinkConnectionParams AS jsonb),
                :sinkColumns, :sinkStagingSchema, :sinkStagingTable,
                :sinkDisableEscape, :sinkDisableTruncate, :mode, :jobs,
                :incrementalWatermarkColumn, :initialWatermarkValue, :createdAt, :updatedAt,
                :fetchSize, :bandwidthThrottling, :verbose
            )
            """;

    private static final String SELECT_COLUMNS = """
            id, name, source_connect, source_user, source_password, source_table, source_where,
            source_auth_mode, source_auth_principal_id, source_auth_login_hint,
            source_auth_client_certificate, source_auth_client_key, source_connection_params,
            source_columns, source_query,
            sink_connect, sink_user, sink_password, sink_table,
            sink_auth_mode, sink_auth_principal_id, sink_auth_login_hint,
            sink_auth_client_certificate, sink_auth_client_key, sink_connection_params,
            sink_columns, sink_staging_schema, sink_staging_table,
            sink_disable_escape, sink_disable_truncate, mode, jobs,
            incremental_watermark_column, initial_watermark_value, created_at, updated_at,
            fetch_size, bandwidth_throttling, "verbose"
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public JobDefinitionRepository(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public JobDefinition insert(JobDefinition definition) {
        UUID id = definition.id() == null ? UUID.randomUUID() : definition.id();
        Instant now = Instant.now();
        Instant createdAt = definition.createdAt() == null ? now : definition.createdAt();
        Instant updatedAt = definition.updatedAt() == null ? createdAt : definition.updatedAt();
        JobDefinition persisted = withPersistenceFields(definition, id, createdAt, updatedAt);

        jdbcTemplate.update(INSERT_SQL, parameters(persisted));
        return persisted;
    }

    public Optional<JobDefinition> findById(UUID id) {
        String sql = "SELECT " + SELECT_COLUMNS + " FROM job_definition WHERE id = :id";
        return queryOne(sql, Map.of("id", id));
    }

    public Optional<JobDefinition> findByName(String name) {
        String sql = "SELECT " + SELECT_COLUMNS + " FROM job_definition WHERE name = :name";
        return queryOne(sql, Map.of("name", name));
    }

    public List<JobDefinition> findAll() {
        String sql = "SELECT " + SELECT_COLUMNS + " FROM job_definition ORDER BY name";
        return jdbcTemplate.query(sql, Map.of(), rowMapper);
    }

    public List<JobDefinition> findPage(int page, int size, Set<UUID> restrictToIds) {
        validatePage(page, size);
        StringBuilder sql = new StringBuilder("SELECT " + SELECT_COLUMNS + " FROM job_definition WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendRestriction(sql, parameters, restrictToIds);
        sql.append(" ORDER BY name, id LIMIT :size OFFSET :offset");
        parameters.addValue("size", size).addValue("offset", (long) page * size);
        return jdbcTemplate.query(sql.toString(), parameters, rowMapper);
    }

    public long count(Set<UUID> restrictToIds) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM job_definition WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendRestriction(sql, parameters, restrictToIds);
        Long count = jdbcTemplate.queryForObject(sql.toString(), parameters, Long.class);
        return count == null ? 0 : count;
    }

    private static void appendRestriction(StringBuilder sql, MapSqlParameterSource parameters,
                                          Set<UUID> restrictToIds) {
        if (restrictToIds != null) {
            sql.append(" AND id = ANY(:restrictToIds)");
            parameters.addValue("restrictToIds", restrictToIds.toArray(UUID[]::new), Types.ARRAY);
        }
    }

    public JobDefinition update(JobDefinition definition) {
        String sql = """
                UPDATE job_definition
                SET source_connect = :sourceConnect, source_user = :sourceUser,
                    source_password = :sourcePassword, source_table = :sourceTable,
                    source_where = :sourceWhere, source_auth_mode = :sourceAuthMode,
                    source_auth_principal_id = :sourceAuthPrincipalId,
                    source_auth_login_hint = :sourceAuthLoginHint,
                    source_auth_client_certificate = :sourceAuthClientCertificate,
                    source_auth_client_key = :sourceAuthClientKey,
                    source_connection_params = CAST(:sourceConnectionParams AS jsonb),
                    source_columns = :sourceColumns, source_query = :sourceQuery,
                    sink_connect = :sinkConnect,
                    sink_user = :sinkUser, sink_password = :sinkPassword,
                    sink_table = :sinkTable, sink_auth_mode = :sinkAuthMode,
                    sink_auth_principal_id = :sinkAuthPrincipalId,
                    sink_auth_login_hint = :sinkAuthLoginHint,
                    sink_auth_client_certificate = :sinkAuthClientCertificate,
                    sink_auth_client_key = :sinkAuthClientKey,
                    sink_connection_params = CAST(:sinkConnectionParams AS jsonb),
                    sink_columns = :sinkColumns, sink_staging_schema = :sinkStagingSchema,
                    sink_staging_table = :sinkStagingTable,
                    sink_disable_escape = :sinkDisableEscape,
                    sink_disable_truncate = :sinkDisableTruncate,
                    mode = :mode, jobs = :jobs,
                    incremental_watermark_column = :incrementalWatermarkColumn,
                    initial_watermark_value = :initialWatermarkValue, updated_at = now(),
                    fetch_size = :fetchSize, bandwidth_throttling = :bandwidthThrottling,
                    "verbose" = :verbose
                WHERE id = :id
                """;
        int updated = jdbcTemplate.update(sql, parameters(definition));
        if (updated != 1) {
            throw new NoSuchElementException("JobDefinition not found: " + definition.id());
        }
        return findById(definition.id()).orElseThrow();
    }

    private static void validatePage(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must not be negative");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size must be positive");
        }
    }

    private Optional<JobDefinition> queryOne(String sql, Map<String, ?> parameters) {
        return jdbcTemplate.query(sql, parameters, rowMapper).stream().findFirst();
    }

    private MapSqlParameterSource parameters(JobDefinition definition) {
        AzureAuthentication sourceAuthentication = definition.sourceAuthentication();
        AzureAuthentication sinkAuthentication = definition.sinkAuthentication();
        return new MapSqlParameterSource()
                .addValue("id", definition.id())
                .addValue("name", definition.name())
                .addValue("sourceConnect", definition.sourceConnect())
                .addValue("sourceUser", definition.sourceUser())
                .addValue("sourcePassword", definition.sourcePassword())
                .addValue("sourceTable", definition.sourceTable())
                .addValue("sourceWhere", definition.sourceWhere())
                .addValue("sourceAuthMode", sourceAuthentication.mode())
                .addValue("sourceAuthPrincipalId", sourceAuthentication.principalId())
                .addValue("sourceAuthLoginHint", sourceAuthentication.loginHint())
                .addValue("sourceAuthClientCertificate", sourceAuthentication.clientCertificate())
                .addValue("sourceAuthClientKey", sourceAuthentication.clientKey())
                .addValue("sourceConnectionParams", serializeConnectionParams(definition.sourceConnectionParams()))
                .addValue("sourceColumns", definition.sourceColumns())
                .addValue("sourceQuery", definition.sourceQuery())
                .addValue("sinkConnect", definition.sinkConnect())
                .addValue("sinkUser", definition.sinkUser())
                .addValue("sinkPassword", definition.sinkPassword())
                .addValue("sinkTable", definition.sinkTable())
                .addValue("sinkAuthMode", sinkAuthentication.mode())
                .addValue("sinkAuthPrincipalId", sinkAuthentication.principalId())
                .addValue("sinkAuthLoginHint", sinkAuthentication.loginHint())
                .addValue("sinkAuthClientCertificate", sinkAuthentication.clientCertificate())
                .addValue("sinkAuthClientKey", sinkAuthentication.clientKey())
                .addValue("sinkConnectionParams", serializeConnectionParams(definition.sinkConnectionParams()))
                .addValue("sinkColumns", definition.sinkColumns())
                .addValue("sinkStagingSchema", definition.sinkStagingSchema())
                .addValue("sinkStagingTable", definition.sinkStagingTable())
                .addValue("sinkDisableEscape", definition.sinkDisableEscape())
                .addValue("sinkDisableTruncate", definition.sinkDisableTruncate())
                .addValue("mode", definition.mode().getModeText())
                .addValue("jobs", definition.jobs())
                .addValue("incrementalWatermarkColumn", definition.incrementalWatermarkColumn())
                .addValue("initialWatermarkValue", definition.initialWatermarkValue())
                .addValue("createdAt", Timestamp.from(definition.createdAt()))
                .addValue("updatedAt", Timestamp.from(definition.updatedAt()))
                .addValue("fetchSize", definition.fetchSize())
                .addValue("bandwidthThrottling", definition.bandwidthThrottling())
                .addValue("verbose", definition.verbose());
    }

    private static JobDefinition withPersistenceFields(JobDefinition definition, UUID id,
                                                       Instant createdAt, Instant updatedAt) {
        return new JobDefinition(
                id, definition.name(), definition.source(), definition.sink(), definition.mode(), definition.jobs(),
                definition.incrementalWatermarkColumn(), definition.initialWatermarkValue(), createdAt, updatedAt,
                definition.fetchSize(), definition.bandwidthThrottling(), definition.verbose());
    }

    private final RowMapper<JobDefinition> rowMapper = new RowMapper<>() {
        @Override
        public JobDefinition mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            return new JobDefinition(
                    resultSet.getObject("id", UUID.class),
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
                    resultSet.getBoolean("verbose"));
        }
    };

    private String serializeConnectionParams(Map<String, String> connectionParams) {
        try {
            return objectMapper.writeValueAsString(connectionParams);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize connection parameters", exception);
        }
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
