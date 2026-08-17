package org.replicadb.server.security.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.domain.JobPermission;
import org.replicadb.server.security.domain.JobPermissionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class JobPermissionRepositoryIT {

    @Autowired
    private JobPermissionRepository repository;

    @Autowired
    private AppUserRepository appUserRepository;

    @Autowired
    private org.replicadb.server.job.persistence.JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE job_permission, job_run, job_definition, app_user CASCADE", Map.of());
    }

    @Test
    void grantsAndChecksPermissionIdempotently() {
        AppUser user = appUserRepository.insert(user("grant-user"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("grant-job"));

        repository.grant(definition.id(), user.id(), JobPermissionType.VIEW);
        repository.grant(definition.id(), user.id(), JobPermissionType.VIEW);

        assertTrue(repository.hasPermission(definition.id(), user.id(), JobPermissionType.VIEW));
        assertFalse(repository.hasPermission(definition.id(), user.id(), JobPermissionType.EDIT));
        assertEquals(1, repository.findByJobDefinitionId(definition.id()).size());
    }

    @Test
    void grantsAllFourPermissionTypes() {
        AppUser user = appUserRepository.insert(user("all-user"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("all-job"));

        repository.grantAll(definition.id(), user.id());

        assertEquals(4, repository.findByJobDefinitionId(definition.id()).size());
        for (JobPermissionType permission : JobPermissionType.values()) {
            assertTrue(repository.hasPermission(definition.id(), user.id(), permission));
        }
    }

    @Test
    void revokesOnePermissionWithoutTouchingOthers() {
        AppUser user = appUserRepository.insert(user("revoke-one-user"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("revoke-one-job"));
        repository.grantAll(definition.id(), user.id());

        repository.revoke(definition.id(), user.id(), JobPermissionType.CANCEL);

        assertFalse(repository.hasPermission(definition.id(), user.id(), JobPermissionType.CANCEL));
        assertTrue(repository.hasPermission(definition.id(), user.id(), JobPermissionType.VIEW));
        assertEquals(3, repository.findByJobDefinitionId(definition.id()).size());
    }

    @Test
    void revokesAllPermissionsIdempotently() {
        AppUser user = appUserRepository.insert(user("revoke-all-user"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("revoke-all-job"));
        repository.grantAll(definition.id(), user.id());

        repository.revokeAll(definition.id(), user.id());
        repository.revokeAll(definition.id(), user.id());

        assertTrue(repository.findByJobDefinitionId(definition.id()).isEmpty());
    }

    @Test
    void findsJobIdsForSpecificPermissionAndEmptyUsers() {
        AppUser user = appUserRepository.insert(user("visible-user"));
        AppUser emptyUser = appUserRepository.insert(user("empty-user"));
        JobDefinition first = jobDefinitionRepository.insert(definition("visible-first"));
        JobDefinition second = jobDefinitionRepository.insert(definition("visible-second"));
        repository.grant(first.id(), user.id(), JobPermissionType.VIEW);
        repository.grant(second.id(), user.id(), JobPermissionType.EXECUTE);

        assertEquals(Set.of(first.id()), repository.findJobIdsWithPermission(user.id(), JobPermissionType.VIEW));
        assertEquals(Set.of(second.id()), repository.findJobIdsWithPermission(user.id(), JobPermissionType.EXECUTE));
        assertEquals(Set.of(), repository.findJobIdsWithPermission(emptyUser.id(), JobPermissionType.VIEW));
    }

    @Test
    void cascadesWhenJobDefinitionIsDeleted() {
        AppUser user = appUserRepository.insert(user("cascade-user"));
        JobDefinition definition = jobDefinitionRepository.insert(definition("cascade-job"));
        repository.grant(definition.id(), user.id(), JobPermissionType.VIEW);

        jdbcTemplate.update("DELETE FROM job_definition WHERE id = :id", Map.of("id", definition.id()));

        assertTrue(repository.findByJobDefinitionId(definition.id()).isEmpty());
    }

    private static AppUser user(String username) {
        return new AppUser(null, username, "password-hash", GlobalRole.VIEWER, true, null, null);
    }

    private static JobDefinition definition(String name) {
        return new JobDefinition(
                null, name, "jdbc:source", "source-user", "${env:SOURCE_PASSWORD}", "source_table", null,
                "jdbc:sink", "sink-user", "${env:SINK_PASSWORD}", "sink_table", ReplicationMode.COMPLETE,
                2, null, null, null, null);
    }
}
