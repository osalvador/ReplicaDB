package org.replicadb.server.audit.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditEvent;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
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
import java.util.UUID;

@Repository
public class AuditEventRepository {

    private static final String SELECT_COLUMNS = """
            id, occurred_at, actor_user_id, actor_username, source_address,
            action, resource_type, resource_id, outcome, detail
            """;

    private static final String INSERT_SQL = """
            INSERT INTO audit_event (
                id, occurred_at, actor_user_id, actor_username, source_address,
                action, resource_type, resource_id, outcome, detail
            ) VALUES (
                :id, :occurredAt, :actorUserId, :actorUsername, :sourceAddress,
                :action, :resourceType, :resourceId, :outcome, CAST(:detail AS jsonb)
            )
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public AuditEventRepository(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public AuditEvent insert(AuditEvent event) {
        UUID id = event.id() == null ? UUID.randomUUID() : event.id();
        Instant occurredAt = event.occurredAt() == null ? Instant.now() : event.occurredAt();
        AuditEvent persisted = new AuditEvent(id, occurredAt, event.actor(), event.action(),
                event.resourceType(), event.resourceId(), event.outcome(), event.detail());

        jdbcTemplate.update(INSERT_SQL, parameters(persisted));
        return persisted;
    }

    public List<AuditEvent> findPage(AuditEventFilter filter, int page, int size) {
        validatePage(page, size);
        AuditEventFilter effectiveFilter = filter == null ? AuditEventFilter.empty() : filter;
        StringBuilder sql = new StringBuilder("SELECT " + SELECT_COLUMNS
                + " FROM audit_event WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, effectiveFilter);
        sql.append(" ORDER BY occurred_at DESC, id DESC LIMIT :size OFFSET :offset");
        parameters.addValue("size", size).addValue("offset", (long) page * size);
        return jdbcTemplate.query(sql.toString(), parameters, ROW_MAPPER);
    }

    public long count(AuditEventFilter filter) {
        AuditEventFilter effectiveFilter = filter == null ? AuditEventFilter.empty() : filter;
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM audit_event WHERE 1 = 1");
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        appendFilters(sql, parameters, effectiveFilter);
        Long count = jdbcTemplate.queryForObject(sql.toString(), parameters, Long.class);
        return count == null ? 0 : count;
    }

    public int deleteOlderThan(int retentionDays) {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("retentionDays must be positive");
        }
        return jdbcTemplate.update("""
                DELETE FROM audit_event
                WHERE occurred_at < now() - (:days * interval '1 day')
                """, Map.of("days", retentionDays));
    }

    private static void appendFilters(StringBuilder sql, MapSqlParameterSource parameters,
                                      AuditEventFilter filter) {
        if (filter.actorUserId() != null) {
            sql.append(" AND actor_user_id = :actorUserId");
            parameters.addValue("actorUserId", filter.actorUserId(), Types.OTHER);
        }
        if (filter.action() != null) {
            sql.append(" AND action = :action");
            parameters.addValue("action", filter.action().name());
        }
        if (filter.resourceType() != null) {
            sql.append(" AND resource_type = :resourceType");
            parameters.addValue("resourceType", filter.resourceType().name());
        }
        if (filter.resourceId() != null) {
            sql.append(" AND resource_id = :resourceId");
            parameters.addValue("resourceId", filter.resourceId());
        }
        if (filter.from() != null) {
            sql.append(" AND occurred_at >= :from");
            parameters.addValue("from", Timestamp.from(filter.from()));
        }
        if (filter.to() != null) {
            sql.append(" AND occurred_at <= :to");
            parameters.addValue("to", Timestamp.from(filter.to()));
        }
    }

    private MapSqlParameterSource parameters(AuditEvent event) {
        return new MapSqlParameterSource()
                .addValue("id", event.id(), Types.OTHER)
                .addValue("occurredAt", Timestamp.from(event.occurredAt()))
                .addValue("actorUserId", event.actor().userId(), Types.OTHER)
                .addValue("actorUsername", event.actor().username())
                .addValue("sourceAddress", event.actor().sourceAddress(), Types.VARCHAR)
                .addValue("action", event.action().name())
                .addValue("resourceType", event.resourceType().name())
                .addValue("resourceId", event.resourceId(), Types.VARCHAR)
                .addValue("outcome", event.outcome().name())
                .addValue("detail", serializeDetail(event.detail()));
    }

    private String serializeDetail(Map<String, String> detail) {
        try {
            return objectMapper.writeValueAsString(detail);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize audit detail", exception);
        }
    }

    private Map<String, String> deserializeDetail(String detail) {
        if (detail == null || detail.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(detail, new TypeReference<Map<String, String>>() { });
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not deserialize audit detail", exception);
        }
    }

    private static void validatePage(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must not be negative");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size must be positive");
        }
    }

    private final RowMapper<AuditEvent> ROW_MAPPER = new RowMapper<>() {
        @Override
        public AuditEvent mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            return new AuditEvent(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getTimestamp("occurred_at").toInstant(),
                    new AuditActor(
                            resultSet.getObject("actor_user_id", UUID.class),
                            resultSet.getString("actor_username"),
                            resultSet.getString("source_address")),
                    AuditAction.valueOf(resultSet.getString("action")),
                    AuditResourceType.valueOf(resultSet.getString("resource_type")),
                    resultSet.getString("resource_id"),
                    AuditOutcome.valueOf(resultSet.getString("outcome")),
                    deserializeDetail(resultSet.getString("detail")));
        }
    };
}
