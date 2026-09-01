package org.replicadb.server.job.persistence;

import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.sql.Types;

@Repository
public class JobDefinitionRepository implements JobDefinitionStore {

    private static final String INSERT_SQL = """
            INSERT INTO job_definition (
                id, name, source_datasource_id, source_table, source_where,
                source_columns, source_query, sink_datasource_id, sink_table,
                sink_columns, sink_staging_schema, sink_staging_table,
                sink_disable_escape, sink_disable_truncate,
                source_datasource_use_enabled, sink_datasource_use_enabled,
                mode, jobs, incremental_watermark_column, initial_watermark_value,
                created_at, updated_at, fetch_size, bandwidth_throttling, "verbose",
                max_attempts, retry_backoff_seconds, automatic_retry_enabled
            ) VALUES (
                :id, :name, :sourceDatasourceId, :sourceTable, :sourceWhere,
                :sourceColumns, :sourceQuery, :sinkDatasourceId, :sinkTable,
                :sinkColumns, :sinkStagingSchema, :sinkStagingTable,
                :sinkDisableEscape, :sinkDisableTruncate,
                :sourceDatasourceUseEnabled, :sinkDatasourceUseEnabled,
                :mode, :jobs, :incrementalWatermarkColumn, :initialWatermarkValue,
                :createdAt, :updatedAt, :fetchSize, :bandwidthThrottling, :verbose,
                :maxAttempts, :retryBackoffSeconds, :automaticRetryEnabled
            )
            """;

        private static final String SELECT_COLUMNS = """
            id, name, source_datasource_id, source_table, source_where,
            source_columns, source_query, sink_datasource_id, sink_table,
            sink_columns, sink_staging_schema, sink_staging_table,
            sink_disable_escape, sink_disable_truncate,
            source_datasource_use_enabled, sink_datasource_use_enabled,
            mode, jobs, incremental_watermark_column, initial_watermark_value,
            created_at, updated_at, fetch_size, bandwidth_throttling, "verbose",
            max_attempts, retry_backoff_seconds, automatic_retry_enabled
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final JobDefinitionRowMapper rowMapper;

    public JobDefinitionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.rowMapper = new JobDefinitionRowMapper();
    }

    public JobDefinition insert(JobDefinition definition) {
        requireDatasourceReferences(definition);
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

    @Override
    public Optional<JobDefinition> findByIdForUpdate(UUID id) {
        String sql = "SELECT " + SELECT_COLUMNS + " FROM job_definition WHERE id = :id FOR UPDATE";
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

    @Transactional
    public JobDefinition update(JobDefinition definition) {
        findByIdForUpdate(definition.id()).orElseThrow(() -> new NoSuchElementException(
                "JobDefinition not found: " + definition.id()));
        requireDatasourceReferences(definition);
        String sql = """
                UPDATE job_definition
                SET source_datasource_id = :sourceDatasourceId,
                    source_table = :sourceTable, source_where = :sourceWhere,
                    source_columns = :sourceColumns, source_query = :sourceQuery,
                    sink_datasource_id = :sinkDatasourceId,
                    sink_table = :sinkTable, sink_columns = :sinkColumns,
                    sink_staging_schema = :sinkStagingSchema,
                    sink_staging_table = :sinkStagingTable,
                    sink_disable_escape = :sinkDisableEscape,
                    sink_disable_truncate = :sinkDisableTruncate,
                    source_datasource_use_enabled = :sourceDatasourceUseEnabled,
                    sink_datasource_use_enabled = :sinkDatasourceUseEnabled,
                    mode = :mode, jobs = :jobs,
                    incremental_watermark_column = :incrementalWatermarkColumn,
                    initial_watermark_value = :initialWatermarkValue, updated_at = now(),
                    fetch_size = :fetchSize, bandwidth_throttling = :bandwidthThrottling,
                    "verbose" = :verbose,
                    max_attempts = :maxAttempts,
                    retry_backoff_seconds = :retryBackoffSeconds,
                    automatic_retry_enabled = :automaticRetryEnabled
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
        return new MapSqlParameterSource()
                .addValue("id", definition.id())
                .addValue("name", definition.name())
                .addValue("sourceDatasourceId", definition.sourceDatasourceId())
                .addValue("sourceTable", definition.sourceTable())
                .addValue("sourceWhere", definition.sourceWhere())
                .addValue("sourceColumns", definition.sourceColumns())
                .addValue("sourceQuery", definition.sourceQuery())
                .addValue("sinkDatasourceId", definition.sinkDatasourceId())
                .addValue("sinkTable", definition.sinkTable())
                .addValue("sinkColumns", definition.sinkColumns())
                .addValue("sinkStagingSchema", definition.sinkStagingSchema())
                .addValue("sinkStagingTable", definition.sinkStagingTable())
                .addValue("sinkDisableEscape", definition.sinkDisableEscape())
                .addValue("sinkDisableTruncate", definition.sinkDisableTruncate())
                .addValue("sourceDatasourceUseEnabled", definition.sourceDatasourceUseEnabled())
                .addValue("sinkDatasourceUseEnabled", definition.sinkDatasourceUseEnabled())
                .addValue("mode", definition.mode().getModeText())
                .addValue("jobs", definition.jobs())
                .addValue("incrementalWatermarkColumn", definition.incrementalWatermarkColumn())
                .addValue("initialWatermarkValue", definition.initialWatermarkValue())
                .addValue("createdAt", Timestamp.from(definition.createdAt()))
                .addValue("updatedAt", Timestamp.from(definition.updatedAt()))
                .addValue("fetchSize", definition.fetchSize())
                .addValue("bandwidthThrottling", definition.bandwidthThrottling())
                .addValue("verbose", definition.verbose())
                .addValue("maxAttempts", definition.maxAttempts())
                .addValue("retryBackoffSeconds", definition.retryBackoffSeconds())
                .addValue("automaticRetryEnabled", definition.automaticRetryEnabled());
    }

    private static JobDefinition withPersistenceFields(JobDefinition definition, UUID id,
                                                       Instant createdAt, Instant updatedAt) {
        return new JobDefinition(
            id, definition.name(), definition.source(), definition.sink(),
            definition.sourceDatasourceUseEnabled(), definition.sinkDatasourceUseEnabled(),
            definition.mode(), definition.jobs(),
                definition.incrementalWatermarkColumn(), definition.initialWatermarkValue(), createdAt, updatedAt,
                definition.fetchSize(), definition.bandwidthThrottling(), definition.verbose(),
                definition.retryPolicy());
            }

    private static void requireDatasourceReferences(JobDefinition definition) {
        if (definition.sourceDatasourceId() == null || definition.sinkDatasourceId() == null) {
            throw new IllegalArgumentException("Managed jobs require source and sink datasource references");
        }
    }

}
