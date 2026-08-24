package org.replicadb.server.security.persistence;

import org.replicadb.server.security.domain.JobPermission;
import org.replicadb.server.security.domain.JobPermissionType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Repository
@Profile("api")
public class JobPermissionRepository {

    private static final String INSERT_SQL = """
            INSERT INTO job_permission (job_definition_id, user_id, permission)
            VALUES (:jobDefinitionId, :userId, :permission)
            ON CONFLICT (job_definition_id, user_id, permission) DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JobPermissionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void grant(UUID jobDefinitionId, UUID userId, JobPermissionType permission) {
        jdbcTemplate.update(INSERT_SQL, parameters(jobDefinitionId, userId, permission));
    }

    public void grantAll(UUID jobDefinitionId, UUID userId) {
        for (JobPermissionType permission : JobPermissionType.values()) {
            grant(jobDefinitionId, userId, permission);
        }
    }

    public void revoke(UUID jobDefinitionId, UUID userId, JobPermissionType permission) {
        jdbcTemplate.update("""
                DELETE FROM job_permission
                WHERE job_definition_id = :jobDefinitionId
                  AND user_id = :userId
                  AND permission = :permission
                """, parameters(jobDefinitionId, userId, permission));
    }

    public void revokeAll(UUID jobDefinitionId, UUID userId) {
        jdbcTemplate.update("""
                DELETE FROM job_permission
                WHERE job_definition_id = :jobDefinitionId AND user_id = :userId
                """, Map.of("jobDefinitionId", jobDefinitionId, "userId", userId));
    }

    public boolean hasPermission(UUID jobDefinitionId, UUID userId, JobPermissionType permission) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM job_permission
                WHERE job_definition_id = :jobDefinitionId
                  AND user_id = :userId
                  AND permission = :permission
                """, parameters(jobDefinitionId, userId, permission), Integer.class);
        return count != null && count > 0;
    }

    public Set<UUID> findJobIdsWithPermission(UUID userId, JobPermissionType permission) {
        List<UUID> jobIds = jdbcTemplate.queryForList("""
                SELECT job_definition_id FROM job_permission
                WHERE user_id = :userId AND permission = :permission
                ORDER BY job_definition_id
                """, parameters(null, userId, permission), UUID.class);
        return new LinkedHashSet<>(jobIds);
    }

    public List<JobPermission> findByJobDefinitionId(UUID jobDefinitionId) {
        return jdbcTemplate.query("""
                SELECT job_definition_id, user_id, permission, created_at
                FROM job_permission
                WHERE job_definition_id = :jobDefinitionId
                ORDER BY user_id, permission
                """, Map.of("jobDefinitionId", jobDefinitionId), ROW_MAPPER);
    }

    private static MapSqlParameterSource parameters(UUID jobDefinitionId, UUID userId,
                                                    JobPermissionType permission) {
        return new MapSqlParameterSource()
                .addValue("jobDefinitionId", jobDefinitionId)
                .addValue("userId", userId)
                .addValue("permission", permission == null ? null : permission.name());
    }

    private static final RowMapper<JobPermission> ROW_MAPPER = new RowMapper<>() {
        @Override
        public JobPermission mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            Timestamp createdAt = resultSet.getTimestamp("created_at");
            return new JobPermission(
                    resultSet.getObject("job_definition_id", UUID.class),
                    resultSet.getObject("user_id", UUID.class),
                    JobPermissionType.valueOf(resultSet.getString("permission")),
                    createdAt == null ? null : createdAt.toInstant());
        }
    };
}
