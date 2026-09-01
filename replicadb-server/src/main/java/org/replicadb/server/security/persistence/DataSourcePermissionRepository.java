package org.replicadb.server.security.persistence;

import org.replicadb.server.job.port.DataSourcePermissionStore;
import org.replicadb.server.security.domain.DataSourcePermission;
import org.replicadb.server.security.domain.DataSourcePermissionType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Repository
public class DataSourcePermissionRepository implements DataSourcePermissionStore {

    private static final String INSERT_SQL = """
            INSERT INTO datasource_permission (datasource_id, user_id, permission)
            VALUES (:datasourceId, :userId, :permission)
            ON CONFLICT (datasource_id, user_id, permission) DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public DataSourcePermissionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void grant(UUID datasourceId, UUID userId, DataSourcePermissionType permission) {
        jdbcTemplate.update(INSERT_SQL, parameters(datasourceId, userId, permission));
    }

    @Override
    @Transactional
    public void replace(UUID datasourceId, UUID userId, Set<DataSourcePermissionType> permissions) {
        revokeAll(datasourceId, userId);
        if (permissions != null) {
            permissions.forEach(permission -> grant(datasourceId, userId, permission));
        }
    }

    @Override
    public void revoke(UUID datasourceId, UUID userId, DataSourcePermissionType permission) {
        jdbcTemplate.update("""
                DELETE FROM datasource_permission
                WHERE datasource_id = :datasourceId
                  AND user_id = :userId
                  AND permission = :permission
                """, parameters(datasourceId, userId, permission));
    }

    @Override
    public void revokeAll(UUID datasourceId, UUID userId) {
        jdbcTemplate.update("""
                DELETE FROM datasource_permission
                WHERE datasource_id = :datasourceId AND user_id = :userId
                """, Map.of("datasourceId", datasourceId, "userId", userId));
    }

    @Override
    public boolean hasPermission(UUID datasourceId, UUID userId, DataSourcePermissionType permission) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM datasource_permission
                WHERE datasource_id = :datasourceId
                  AND user_id = :userId
                  AND permission = :permission
                """, parameters(datasourceId, userId, permission), Integer.class);
        return count != null && count > 0;
    }

    @Override
    public Set<UUID> findDatasourceIdsWithPermission(UUID userId, DataSourcePermissionType permission) {
        List<UUID> ids = jdbcTemplate.queryForList("""
                SELECT datasource_id FROM datasource_permission
                WHERE user_id = :userId AND permission = :permission
                ORDER BY datasource_id
                """, parameters(null, userId, permission), UUID.class);
        return new LinkedHashSet<>(ids);
    }

    @Override
    public List<DataSourcePermission> findByDatasourceId(UUID datasourceId) {
        return jdbcTemplate.query("""
                SELECT datasource_id, user_id, permission, created_at
                FROM datasource_permission
                WHERE datasource_id = :datasourceId
                ORDER BY user_id, permission
                """, Map.of("datasourceId", datasourceId), ROW_MAPPER);
    }

    private static MapSqlParameterSource parameters(UUID datasourceId, UUID userId,
                                                    DataSourcePermissionType permission) {
        return new MapSqlParameterSource()
                .addValue("datasourceId", datasourceId)
                .addValue("userId", userId)
                .addValue("permission", permission == null ? null : permission.name());
    }

    private static final RowMapper<DataSourcePermission> ROW_MAPPER = new RowMapper<>() {
        @Override
        public DataSourcePermission mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            Timestamp createdAt = resultSet.getTimestamp("created_at");
            return new DataSourcePermission(
                    resultSet.getObject("datasource_id", UUID.class),
                    resultSet.getObject("user_id", UUID.class),
                    DataSourcePermissionType.valueOf(resultSet.getString("permission")),
                    createdAt == null ? null : createdAt.toInstant());
        }
    };
}
