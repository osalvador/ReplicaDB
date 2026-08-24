package org.replicadb.server.security.persistence;

import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

@Repository
@Profile("api")
public class AppUserRepository {

    private static final String SELECT_COLUMNS = """
            id, username, password_hash, role, enabled, created_at, updated_at
            """;

    private static final String INSERT_SQL = """
            INSERT INTO app_user (
                id, username, password_hash, role, enabled, created_at, updated_at
            ) VALUES (
                :id, :username, :passwordHash, :role, :enabled, :createdAt, :updatedAt
            )
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public AppUserRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public AppUser insert(AppUser user) {
        UUID id = user.id() == null ? UUID.randomUUID() : user.id();
        Instant now = Instant.now();
        Instant createdAt = user.createdAt() == null ? now : user.createdAt();
        Instant updatedAt = user.updatedAt() == null ? createdAt : user.updatedAt();
        AppUser persisted = withPersistenceFields(user, id, createdAt, updatedAt);

        jdbcTemplate.update(INSERT_SQL, parameters(persisted));
        return persisted;
    }

    public Optional<AppUser> findById(UUID id) {
        return queryOne("SELECT " + SELECT_COLUMNS + " FROM app_user WHERE id = :id", Map.of("id", id));
    }

    public Optional<AppUser> findByUsername(String username) {
        return queryOne("SELECT " + SELECT_COLUMNS + " FROM app_user WHERE username = :username",
                Map.of("username", username));
    }

    public List<AppUser> findPage(int page, int size) {
        validatePage(page, size);
        String sql = "SELECT " + SELECT_COLUMNS
                + " FROM app_user ORDER BY username, id LIMIT :size OFFSET :offset";
        return jdbcTemplate.query(sql, new MapSqlParameterSource()
                .addValue("size", size)
                .addValue("offset", (long) page * size), ROW_MAPPER);
    }

    public long count() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM app_user", Map.of(), Long.class);
        return count == null ? 0 : count;
    }

    public long countByRole(GlobalRole role) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM app_user WHERE role = :role",
                Map.of("role", role.name()), Long.class);
        return count == null ? 0 : count;
    }

    public AppUser update(AppUser user) {
        String sql = """
                UPDATE app_user
                SET password_hash = :passwordHash, role = :role, enabled = :enabled, updated_at = now()
                WHERE id = :id
                """;
        int updated = jdbcTemplate.update(sql, parameters(user));
        if (updated != 1) {
            throw new NoSuchElementException("AppUser not found: " + user.id());
        }
        return findById(user.id()).orElseThrow();
    }

    private Optional<AppUser> queryOne(String sql, Map<String, ?> parameters) {
        return jdbcTemplate.query(sql, parameters, ROW_MAPPER).stream().findFirst();
    }

    private static void validatePage(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must not be negative");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size must be positive");
        }
    }

    private static MapSqlParameterSource parameters(AppUser user) {
        return new MapSqlParameterSource()
                .addValue("id", user.id())
                .addValue("username", user.username())
                .addValue("passwordHash", user.passwordHash())
                .addValue("role", user.role().name())
                .addValue("enabled", user.enabled())
                .addValue("createdAt", timestamp(user.createdAt()))
                .addValue("updatedAt", timestamp(user.updatedAt()));
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static AppUser withPersistenceFields(AppUser user, UUID id, Instant createdAt, Instant updatedAt) {
        return new AppUser(id, user.username(), user.passwordHash(), user.role(), user.enabled(), createdAt, updatedAt);
    }

    private static final RowMapper<AppUser> ROW_MAPPER = new RowMapper<>() {
        @Override
        public AppUser mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            return new AppUser(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getString("username"),
                    resultSet.getString("password_hash"),
                    GlobalRole.valueOf(resultSet.getString("role")),
                    resultSet.getBoolean("enabled"),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant());
        }
    };
}
