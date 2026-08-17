package org.replicadb.server.job.persistence;

import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.JobDefinition;
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
                sink_connect, sink_user, sink_password, sink_table, mode, jobs,
                incremental_watermark_column, initial_watermark_value, created_at, updated_at
            ) VALUES (
                :id, :name, :sourceConnect, :sourceUser, :sourcePassword, :sourceTable, :sourceWhere,
                :sinkConnect, :sinkUser, :sinkPassword, :sinkTable, :mode, :jobs,
                :incrementalWatermarkColumn, :initialWatermarkValue, :createdAt, :updatedAt
            )
            """;

    private static final String SELECT_COLUMNS = """
            id, name, source_connect, source_user, source_password, source_table, source_where,
            sink_connect, sink_user, sink_password, sink_table, mode, jobs,
            incremental_watermark_column, initial_watermark_value, created_at, updated_at
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JobDefinitionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
        return jdbcTemplate.query(sql, Map.of(), ROW_MAPPER);
    }

    public List<JobDefinition> findPage(int page, int size, Set<UUID> restrictToIds) {
        validatePage(page, size);
        StringBuilder sql = new StringBuilder("SELECT " + SELECT_COLUMNS + " FROM job_definition WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendRestriction(sql, parameters, restrictToIds);
        sql.append(" ORDER BY name, id LIMIT :size OFFSET :offset");
        parameters.addValue("size", size).addValue("offset", (long) page * size);
        return jdbcTemplate.query(sql.toString(), parameters, ROW_MAPPER);
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
                    source_where = :sourceWhere, sink_connect = :sinkConnect,
                    sink_user = :sinkUser, sink_password = :sinkPassword,
                    sink_table = :sinkTable, mode = :mode, jobs = :jobs,
                    incremental_watermark_column = :incrementalWatermarkColumn,
                    initial_watermark_value = :initialWatermarkValue, updated_at = now()
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
        return jdbcTemplate.query(sql, parameters, ROW_MAPPER).stream().findFirst();
    }

    private static MapSqlParameterSource parameters(JobDefinition definition) {
        return new MapSqlParameterSource()
                .addValue("id", definition.id())
                .addValue("name", definition.name())
                .addValue("sourceConnect", definition.sourceConnect())
                .addValue("sourceUser", definition.sourceUser())
                .addValue("sourcePassword", definition.sourcePassword())
                .addValue("sourceTable", definition.sourceTable())
                .addValue("sourceWhere", definition.sourceWhere())
                .addValue("sinkConnect", definition.sinkConnect())
                .addValue("sinkUser", definition.sinkUser())
                .addValue("sinkPassword", definition.sinkPassword())
                .addValue("sinkTable", definition.sinkTable())
                .addValue("mode", definition.mode().getModeText())
                .addValue("jobs", definition.jobs())
                .addValue("incrementalWatermarkColumn", definition.incrementalWatermarkColumn())
                .addValue("initialWatermarkValue", definition.initialWatermarkValue())
                .addValue("createdAt", Timestamp.from(definition.createdAt()))
                .addValue("updatedAt", Timestamp.from(definition.updatedAt()));
    }

    private static JobDefinition withPersistenceFields(JobDefinition definition, UUID id,
                                                       Instant createdAt, Instant updatedAt) {
        return new JobDefinition(
                id, definition.name(), definition.sourceConnect(), definition.sourceUser(), definition.sourcePassword(),
                definition.sourceTable(), definition.sourceWhere(), definition.sinkConnect(), definition.sinkUser(),
                definition.sinkPassword(), definition.sinkTable(), definition.mode(), definition.jobs(),
                definition.incrementalWatermarkColumn(), definition.initialWatermarkValue(), createdAt, updatedAt);
    }

    private static final RowMapper<JobDefinition> ROW_MAPPER = new RowMapper<>() {
        @Override
        public JobDefinition mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            return new JobDefinition(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getString("name"),
                    resultSet.getString("source_connect"),
                    resultSet.getString("source_user"),
                    resultSet.getString("source_password"),
                    resultSet.getString("source_table"),
                    resultSet.getString("source_where"),
                    resultSet.getString("sink_connect"),
                    resultSet.getString("sink_user"),
                    resultSet.getString("sink_password"),
                    resultSet.getString("sink_table"),
                    parseMode(resultSet.getString("mode")),
                    resultSet.getInt("jobs"),
                    resultSet.getString("incremental_watermark_column"),
                    resultSet.getString("initial_watermark_value"),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant());
        }
    };

    private static ReplicationMode parseMode(String modeText) {
        for (ReplicationMode mode : ReplicationMode.values()) {
            if (mode.getModeText().equals(modeText.toLowerCase(Locale.ROOT))) {
                return mode;
            }
        }
        throw new IllegalStateException("Unknown replication mode: " + modeText);
    }
}
