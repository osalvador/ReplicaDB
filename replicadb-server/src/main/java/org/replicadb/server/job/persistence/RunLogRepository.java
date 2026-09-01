package org.replicadb.server.job.persistence;

import org.replicadb.server.job.domain.RunLog;
import org.replicadb.server.job.port.RunLogStore;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class RunLogRepository implements RunLogStore {

    private static final String COLUMNS = "run_id, content, truncated, captured_size, format_version, "
            + "captured_at, updated_at";

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public RunLogRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void replaceTerminal(RunLog runLog) {
        String sql = """
                INSERT INTO run_log (run_id, content, truncated, captured_size, format_version,
                                     captured_at, updated_at)
                VALUES (:runId, :content, :truncated, :capturedSize, :formatVersion,
                        :capturedAt, :updatedAt)
                ON CONFLICT (run_id) DO UPDATE SET
                    content = EXCLUDED.content,
                    truncated = EXCLUDED.truncated,
                    captured_size = EXCLUDED.captured_size,
                    format_version = EXCLUDED.format_version,
                    captured_at = EXCLUDED.captured_at,
                    updated_at = EXCLUDED.updated_at
                """;
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("runId", runLog.runId())
                .addValue("content", runLog.content())
                .addValue("truncated", runLog.truncated())
                .addValue("capturedSize", runLog.capturedSize())
                .addValue("formatVersion", runLog.formatVersion())
                .addValue("capturedAt", Timestamp.from(runLog.capturedAt()))
                .addValue("updatedAt", Timestamp.from(runLog.updatedAt()));
        jdbcTemplate.update(sql, parameters);
    }

    @Override
    public Optional<RunLog> findByRunId(UUID runId) {
        String sql = "SELECT " + COLUMNS + " FROM run_log WHERE run_id = :runId";
        return jdbcTemplate.query(sql, Map.of("runId", runId), (resultSet, rowNum) -> new RunLog(
                resultSet.getObject("run_id", UUID.class),
                resultSet.getString("content"),
                resultSet.getBoolean("truncated"),
                resultSet.getInt("captured_size"),
                resultSet.getInt("format_version"),
                resultSet.getTimestamp("captured_at").toInstant(),
                resultSet.getTimestamp("updated_at").toInstant())).stream().findFirst();
    }

    public int deleteOlderThan(int retentionDays, int batchSize) {
        if (retentionDays < 1 || batchSize < 1) {
            throw new IllegalArgumentException("retentionDays and batchSize must be positive");
        }
        String sql = """
                DELETE FROM run_log
                WHERE run_id IN (
                    SELECT l.run_id
                    FROM run_log l
                    JOIN job_run r ON r.id = l.run_id
                    WHERE r.finished_at IS NOT NULL
                      AND r.finished_at < now() - (:retentionDays * interval '1 day')
                      AND r.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'RETRY_SCHEDULED')
                    ORDER BY r.finished_at, l.run_id
                    LIMIT :batchSize
                )
                """;
        return jdbcTemplate.update(sql, Map.of("retentionDays", retentionDays, "batchSize", batchSize));
    }
}
