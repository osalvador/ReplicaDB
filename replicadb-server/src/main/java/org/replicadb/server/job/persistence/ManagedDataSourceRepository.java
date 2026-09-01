package org.replicadb.server.job.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class ManagedDataSourceRepository implements ManagedDataSourceStore {

    private static final String FULL_COLUMNS = """
            id, name, connector_type, safe_connect_display, technical_params,
            encrypted_security, security_format_version, encryption_algorithm, key_version,
            created_at, updated_at
            """;

    private static final String SUMMARY_COLUMNS = """
            id, name, connector_type, safe_connect_display, technical_params,
            octet_length(encrypted_security) > 0 AS security_configured,
            security_format_version, encryption_algorithm, key_version,
            created_at, updated_at
            """;

    private static final String INSERT_SQL = """
            INSERT INTO managed_datasource (
                id, name, connector_type, safe_connect_display, technical_params,
                encrypted_security, security_format_version, encryption_algorithm, key_version,
                created_at, updated_at
            ) VALUES (
                :id, :name, :connectorType, :safeConnectDisplay, CAST(:technicalParams AS jsonb),
                :encryptedSecurity, :securityFormatVersion, :encryptionAlgorithm, :keyVersion,
                :createdAt, :updatedAt
            )
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public ManagedDataSourceRepository(NamedParameterJdbcTemplate jdbcTemplate,
                                       ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public ManagedDataSource insert(ManagedDataSource dataSource) {
        Instant now = Instant.now();
        Instant createdAt = dataSource.createdAt() == null ? now : dataSource.createdAt();
        Instant updatedAt = dataSource.updatedAt() == null ? createdAt : dataSource.updatedAt();
        ManagedDataSource persisted = withPersistenceFields(dataSource, createdAt, updatedAt);
        jdbcTemplate.update(INSERT_SQL, parameters(persisted));
        return findById(persisted.id()).orElseThrow(() -> new IllegalStateException(
                "Created managed datasource could not be loaded: " + persisted.id()));
    }

    @Override
    public ManagedDataSource update(ManagedDataSource dataSource) {
        String sql = """
                UPDATE managed_datasource
                SET name = :name, connector_type = :connectorType,
                    safe_connect_display = :safeConnectDisplay,
                    technical_params = CAST(:technicalParams AS jsonb),
                    encrypted_security = :encryptedSecurity,
                    security_format_version = :securityFormatVersion,
                    encryption_algorithm = :encryptionAlgorithm,
                    key_version = :keyVersion,
                    updated_at = now()
                WHERE id = :id
                """;
        if (jdbcTemplate.update(sql, parameters(dataSource)) != 1) {
            throw new NoSuchElementException("ManagedDataSource not found: " + dataSource.id());
        }
        return findById(dataSource.id()).orElseThrow();
    }

    @Override
    public Optional<ManagedDataSource> findById(UUID id) {
        return queryOne("SELECT " + FULL_COLUMNS
                + " FROM managed_datasource WHERE id = :id", Map.of("id", id), FULL_ROW_MAPPER);
    }

    @Override
    public Optional<ManagedDataSource> findByIdForUpdate(UUID id) {
        return queryOne("SELECT " + FULL_COLUMNS
                + " FROM managed_datasource WHERE id = :id FOR UPDATE", Map.of("id", id), FULL_ROW_MAPPER);
    }

    @Override
    public Optional<ManagedDataSourceSummary> findSummaryById(UUID id) {
        return queryOne("SELECT " + SUMMARY_COLUMNS
                + " FROM managed_datasource WHERE id = :id", Map.of("id", id), SUMMARY_ROW_MAPPER);
    }

    @Override
    public Optional<ManagedDataSource> findByName(String name) {
        return queryOne("SELECT " + FULL_COLUMNS
                + " FROM managed_datasource WHERE name = :name", Map.of("name", name), FULL_ROW_MAPPER);
    }

    @Override
    public List<ManagedDataSourceSummary> findPage(int page, int size,
                                                   Set<UUID> restrictToIds,
                                                   Set<ConnectorType> restrictToTypes) {
        validatePage(page, size);
        StringBuilder sql = new StringBuilder("SELECT " + SUMMARY_COLUMNS
                + " FROM managed_datasource WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, restrictToIds, restrictToTypes);
        sql.append(" ORDER BY name, id LIMIT :size OFFSET :offset");
        parameters.addValue("size", size).addValue("offset", (long) page * size);
        return jdbcTemplate.query(sql.toString(), parameters, SUMMARY_ROW_MAPPER);
    }

    @Override
    public long count(Set<UUID> restrictToIds, Set<ConnectorType> restrictToTypes) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM managed_datasource WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, restrictToIds, restrictToTypes);
        Long count = jdbcTemplate.queryForObject(sql.toString(), parameters, Long.class);
        return count == null ? 0 : count;
    }

    @Override
    public DeleteResult delete(UUID id) {
        if (countJobReferences(id) > 0) {
            return DeleteResult.REFERENCED;
        }
        try {
            return jdbcTemplate.update("DELETE FROM managed_datasource WHERE id = :id", Map.of("id", id)) == 1
                    ? DeleteResult.DELETED : DeleteResult.NOT_FOUND;
        } catch (DataIntegrityViolationException exception) {
            return DeleteResult.REFERENCED;
        }
    }

    @Override
    public long countJobReferences(UUID id) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM job_definition
                WHERE source_datasource_id = :id OR sink_datasource_id = :id
                """, Map.of("id", id), Long.class);
        return count == null ? 0 : count;
    }

    private void appendFilters(StringBuilder sql, MapSqlParameterSource parameters,
                               Set<UUID> restrictToIds, Set<ConnectorType> restrictToTypes) {
        if (restrictToIds != null) {
            if (restrictToIds.isEmpty()) {
                sql.append(" AND FALSE");
            } else {
                sql.append(" AND id = ANY(:restrictToIds)");
                parameters.addValue("restrictToIds", restrictToIds.toArray(UUID[]::new), Types.ARRAY);
            }
        }
        if (restrictToTypes != null) {
            if (restrictToTypes.isEmpty()) {
                sql.append(" AND FALSE");
            } else {
                sql.append(" AND connector_type IN (:restrictToTypes)");
                parameters.addValue("restrictToTypes", restrictToTypes.stream()
                        .map(ConnectorType::getWireValue).toList());
            }
        }
    }

    private MapSqlParameterSource parameters(ManagedDataSource dataSource) {
        return new MapSqlParameterSource()
                .addValue("id", dataSource.id())
                .addValue("name", dataSource.name())
                .addValue("connectorType", dataSource.connectorType().getWireValue())
                .addValue("safeConnectDisplay", dataSource.safeConnectDisplay())
                .addValue("technicalParams", serializeTechnicalParams(dataSource.technicalParams()))
                .addValue("encryptedSecurity", dataSource.encryptedSecurity(), Types.BINARY)
                .addValue("securityFormatVersion", dataSource.securityFormatVersion())
                .addValue("encryptionAlgorithm", dataSource.encryptionAlgorithm())
                .addValue("keyVersion", dataSource.keyVersion())
                .addValue("createdAt", timestamp(dataSource.createdAt()))
                .addValue("updatedAt", timestamp(dataSource.updatedAt()));
    }

    private String serializeTechnicalParams(Map<String, String> technicalParams) {
        try {
            return objectMapper.writeValueAsString(technicalParams);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize datasource technical parameters", exception);
        }
    }

    private Map<String, String> deserializeTechnicalParams(String technicalParams) {
        if (technicalParams == null || technicalParams.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, String> result = objectMapper.readValue(technicalParams,
                    new TypeReference<Map<String, String>>() { });
            return result == null ? Map.of() : result;
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not deserialize datasource technical parameters", exception);
        }
    }

    private <T> Optional<T> queryOne(String sql, Map<String, ?> parameters,
                                     RowMapper<T> rowMapper) {
        return jdbcTemplate.query(sql, parameters, rowMapper).stream().findFirst();
    }

    private static ManagedDataSource withPersistenceFields(ManagedDataSource dataSource,
                                                            Instant createdAt, Instant updatedAt) {
        return new ManagedDataSource(dataSource.id(), dataSource.name(), dataSource.connectorType(),
                dataSource.safeConnectDisplay(), dataSource.technicalParams(), dataSource.encryptedSecurity(),
                dataSource.securityFormatVersion(), dataSource.encryptionAlgorithm(), dataSource.keyVersion(),
                createdAt, updatedAt);
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static void validatePage(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must not be negative");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size must be positive");
        }
    }

    private final RowMapper<ManagedDataSource> FULL_ROW_MAPPER = (resultSet, rowNum) ->
            new ManagedDataSource(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getString("name"),
                    ConnectorType.fromWireValue(resultSet.getString("connector_type")),
                    resultSet.getString("safe_connect_display"),
                    deserializeTechnicalParams(resultSet.getString("technical_params")),
                    resultSet.getBytes("encrypted_security"),
                    resultSet.getInt("security_format_version"),
                    resultSet.getString("encryption_algorithm"),
                    resultSet.getString("key_version"),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant());

    private final RowMapper<ManagedDataSourceSummary> SUMMARY_ROW_MAPPER = (resultSet, rowNum) ->
            new ManagedDataSourceSummary(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getString("name"),
                    ConnectorType.fromWireValue(resultSet.getString("connector_type")),
                    resultSet.getString("safe_connect_display"),
                    deserializeTechnicalParams(resultSet.getString("technical_params")),
                    resultSet.getBoolean("security_configured"),
                    resultSet.getInt("security_format_version"),
                    resultSet.getString("encryption_algorithm"),
                    resultSet.getString("key_version"),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant());
}
