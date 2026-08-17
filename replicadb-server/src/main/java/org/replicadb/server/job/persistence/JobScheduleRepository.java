package org.replicadb.server.job.persistence;

import org.replicadb.server.job.domain.JobSchedule;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JobScheduleRepository {

    private static final String SELECT_COLUMNS = """
            job_definition_id, cron_expression, time_zone, enabled, created_at, updated_at
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JobScheduleRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JobSchedule upsert(JobSchedule schedule) {
        String sql = """
                INSERT INTO job_schedule (job_definition_id, cron_expression, time_zone, enabled)
                VALUES (:jobDefinitionId, :cronExpression, :timeZone, :enabled)
                ON CONFLICT (job_definition_id) DO UPDATE
                SET cron_expression = EXCLUDED.cron_expression,
                    time_zone = EXCLUDED.time_zone,
                    enabled = EXCLUDED.enabled,
                    updated_at = now()
                RETURNING """ + " " + SELECT_COLUMNS;
        return jdbcTemplate.query(sql, parameters(schedule), ROW_MAPPER).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Could not persist schedule for job definition " + schedule.jobDefinitionId()));
    }

    public Optional<JobSchedule> findByJobDefinitionId(UUID jobDefinitionId) {
        String sql = "SELECT " + SELECT_COLUMNS
                + " FROM job_schedule WHERE job_definition_id = :jobDefinitionId";
        return jdbcTemplate.query(sql, Map.of("jobDefinitionId", jobDefinitionId), ROW_MAPPER)
                .stream()
                .findFirst();
    }

    public List<JobSchedule> findAllEnabled() {
        String sql = "SELECT " + SELECT_COLUMNS
                + " FROM job_schedule WHERE enabled = true ORDER BY job_definition_id";
        return jdbcTemplate.query(sql, Map.of(), ROW_MAPPER);
    }

    public boolean delete(UUID jobDefinitionId) {
        String sql = "DELETE FROM job_schedule WHERE job_definition_id = :jobDefinitionId";
        return jdbcTemplate.update(sql, Map.of("jobDefinitionId", jobDefinitionId)) == 1;
    }

    private static Map<String, ?> parameters(JobSchedule schedule) {
        return Map.of(
                "jobDefinitionId", schedule.jobDefinitionId(),
                "cronExpression", schedule.cronExpression(),
                "timeZone", schedule.timeZone(),
                "enabled", schedule.enabled());
    }

    private static final RowMapper<JobSchedule> ROW_MAPPER = new RowMapper<>() {
        @Override
        public JobSchedule mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            return new JobSchedule(
                    resultSet.getObject("job_definition_id", UUID.class),
                    resultSet.getString("cron_expression"),
                    resultSet.getString("time_zone"),
                    resultSet.getBoolean("enabled"),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant());
        }
    };
}