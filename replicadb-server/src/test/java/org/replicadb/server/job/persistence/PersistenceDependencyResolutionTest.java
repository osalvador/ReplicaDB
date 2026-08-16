package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class PersistenceDependencyResolutionTest {

    @Test
    void resolvesPersistenceClasses() {
        assertDoesNotThrow(() -> Class.forName("org.flywaydb.core.Flyway"));
        assertDoesNotThrow(() -> Class.forName("org.postgresql.Driver"));
        assertDoesNotThrow(() -> Class.forName(
                "org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate"));
    }
}