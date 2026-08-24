package org.replicadb.server.job.persistence;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class RunTriggerIdempotencyRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public RunTriggerIdempotencyRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<UUID> findValidRunId(String idempotencyKey) {
        return findValidEntry(idempotencyKey).map(IdempotencyEntry::runId);
        }

        public Optional<IdempotencyEntry> findValidEntry(String idempotencyKey) {
        String sql = """
            SELECT job_definition_id, run_id
                FROM run_trigger_idempotency
                WHERE idempotency_key = :idempotencyKey
                  AND created_at > now() - interval '24 hours'
            FOR UPDATE
                """;
        return jdbcTemplate.query(sql, Map.of("idempotencyKey", idempotencyKey),
            (resultSet, rowNum) -> new IdempotencyEntry(
                resultSet.getObject("job_definition_id", UUID.class),
                resultSet.getObject("run_id", UUID.class)))
                .stream()
                .findFirst();
    }

        public Optional<IdempotencyEntry> reserve(String idempotencyKey, UUID jobDefinitionId, UUID runId) {
        String sql = """
            INSERT INTO run_trigger_idempotency (idempotency_key, job_definition_id, run_id, created_at)
            VALUES (:idempotencyKey, :jobDefinitionId, :runId, now())
            ON CONFLICT (idempotency_key) DO UPDATE
            SET job_definition_id = EXCLUDED.job_definition_id,
                run_id = EXCLUDED.run_id,
                created_at = EXCLUDED.created_at
            WHERE run_trigger_idempotency.created_at <= now() - interval '24 hours'
            RETURNING job_definition_id, run_id
            """;
        return jdbcTemplate.query(sql, Map.of(
                "idempotencyKey", idempotencyKey,
                "jobDefinitionId", jobDefinitionId,
                "runId", runId),
            (resultSet, rowNum) -> new IdempotencyEntry(
                resultSet.getObject("job_definition_id", UUID.class),
                resultSet.getObject("run_id", UUID.class)))
            .stream()
            .findFirst();
        }

    public void upsert(String idempotencyKey, UUID jobDefinitionId, UUID runId) {
        String sql = """
                INSERT INTO run_trigger_idempotency (idempotency_key, job_definition_id, run_id, created_at)
                VALUES (:idempotencyKey, :jobDefinitionId, :runId, now())
                ON CONFLICT (idempotency_key) DO UPDATE
                SET job_definition_id = EXCLUDED.job_definition_id,
                    run_id = EXCLUDED.run_id,
                    created_at = EXCLUDED.created_at
                """;
        jdbcTemplate.update(sql, Map.of(
                "idempotencyKey", idempotencyKey,
                "jobDefinitionId", jobDefinitionId,
                "runId", runId));
    }

    public int deleteExpired() {
        return jdbcTemplate.update("""
                DELETE FROM run_trigger_idempotency
                WHERE created_at < now() - interval '48 hours'
                """, Map.of());
    }

    public record IdempotencyEntry(UUID jobDefinitionId, UUID runId) {
    }
}
