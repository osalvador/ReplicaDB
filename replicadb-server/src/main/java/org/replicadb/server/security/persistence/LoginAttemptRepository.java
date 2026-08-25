package org.replicadb.server.security.persistence;

import org.replicadb.server.security.auth.LoginAttemptReservation;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

@Repository
@Profile("api")
public class LoginAttemptRepository {

    private static final int MAX_ATTEMPTS = 5;
    private static final String EXPIRED_DELETE_SQL = """
            DELETE FROM login_attempt
            WHERE attempted_at <= now() - interval '15 minutes'
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public LoginAttemptRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public Optional<LoginAttemptReservation> reserve(String username, String remoteAddress) {
        String usernameKey = key("user", username);
        String addressKey = key("addr", remoteAddress);
        lockKeys(usernameKey, addressKey);
        jdbcTemplate.update(EXPIRED_DELETE_SQL, Map.of());
        if (count(usernameKey) >= MAX_ATTEMPTS || count(addressKey) >= MAX_ATTEMPTS) {
            return Optional.empty();
        }

        UUID reservationId = UUID.randomUUID();
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("reservationId", reservationId, Types.OTHER)
                .addValue("usernameKey", usernameKey)
                .addValue("addressKey", addressKey);
        jdbcTemplate.update("""
                INSERT INTO login_attempt (reservation_id, throttle_key, status, attempted_at)
                VALUES (:reservationId, :usernameKey, 'PENDING', now()),
                       (:reservationId, :addressKey, 'PENDING', now())
                """, parameters);
        return Optional.of(new LoginAttemptReservation(reservationId, usernameKey, addressKey));
    }

    @Transactional
    public boolean recordFailure(LoginAttemptReservation reservation) {
        lockKeys(reservation.usernameKey(), reservation.addressKey());
        int updated = jdbcTemplate.update("""
                UPDATE login_attempt
                SET status = 'FAILED'
                WHERE reservation_id = :reservationId AND status = 'PENDING'
                """, Map.of("reservationId", reservation.id()));
        return updated == 2;
    }

    @Transactional
    public boolean recordSuccess(LoginAttemptReservation reservation) {
        lockKeys(reservation.usernameKey(), reservation.addressKey());
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("reservationId", reservation.id(), Types.OTHER)
                .addValue("keys", List.of(reservation.usernameKey(), reservation.addressKey()));
        int deleted = jdbcTemplate.update("""
                DELETE FROM login_attempt
                WHERE throttle_key IN (:keys)
                  AND EXISTS (
                      SELECT 1 FROM login_attempt
                      WHERE reservation_id = :reservationId AND status = 'PENDING'
                  )
                """, parameters);
        return deleted > 0;
    }

    @Transactional
    public int deleteExpired() {
        return jdbcTemplate.update(EXPIRED_DELETE_SQL, Map.of());
    }

    private long count(String throttleKey) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM login_attempt
                WHERE throttle_key = :throttleKey
                  AND attempted_at > now() - interval '15 minutes'
                  AND status IN ('PENDING', 'FAILED')
                """, Map.of("throttleKey", throttleKey), Long.class);
        return count == null ? 0 : count;
    }

    private void lockKeys(String firstKey, String secondKey) {
        Stream.of(firstKey, secondKey).sorted().forEach(this::lockKey);
    }

    private void lockKey(String throttleKey) {
        jdbcTemplate.query("""
                SELECT pg_advisory_xact_lock(hashtextextended(:throttleKey, CAST(0 AS BIGINT)))
                """, Map.of("throttleKey", throttleKey), resultSet -> null);
    }

    private static String key(String type, String value) {
        return type + ":" + (value == null ? "<unknown>" : value);
    }
}
