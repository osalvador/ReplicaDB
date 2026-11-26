package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.ManagedDataSourceTestSupport;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.ManagedDataSourceRepository;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@Testcontainers
class MariaDbReplicationIT {

    @Container
    static final MariaDBContainer<?> TLS = new MariaDBContainer<>("mariadb:11.4")
            .withCommand("--require-secure-transport=ON", "--local-infile=1");

    @Container
    static final MariaDBContainer<?> LEGACY = new MariaDBContainer<>("mariadb:10.2")
            .withUrlParam("sslMode", "disable");

    @Autowired
    private JobExecutionService executionService;

    @Autowired
    private JobDefinitionRepository jobDefinitions;

    @Autowired
    private JobRunRepository jobRuns;

    @Autowired
    private ManagedDataSourceRepository dataSources;

    @Autowired
    private SecretProtectionService protectionService;

    @Test
    void replicatesFromAndToMariaDbWithDefaultTls() throws SQLException {
        assertReplication("tls", TLS, Map.of(), true);
    }

    @Test
    void replicatesFromAndToMariaDb102WithExplicitTlsOptOut() throws SQLException {
        assertReplication("legacy", LEGACY, Map.of("sslMode", "disable"), false);
    }

    private void assertReplication(String name, MariaDBContainer<?> database,
                                   Map<String, String> connectionParams, boolean tls) throws SQLException {
        String fixtureUrl = database.getJdbcUrl();
        String username = database.getUsername();
        String password = database.getPassword();
        try (var connection = DriverManager.getConnection(fixtureUrl, username, password);
             var statement = connection.createStatement()) {
            try (var status = statement.executeQuery("SHOW SESSION STATUS LIKE 'Ssl_cipher'")) {
                assertTrue(status.next());
                assertEquals(tls, !status.getString(2).isBlank(), "Fixture connection TLS status");
            }
            statement.execute("CREATE TABLE replica_source (id INT PRIMARY KEY, payload VARCHAR(30))");
            statement.execute("CREATE TABLE replica_sink (id INT PRIMARY KEY, payload VARCHAR(30))");
            statement.execute("INSERT INTO replica_source VALUES (1, 'replicated row')");
        }

        String managedUrl = fixtureUrl.split("\\?", 2)[0];
        var credentials = Map.of("user", username, "password", password);
        var sourceId = ManagedDataSourceTestSupport.insert(dataSources, protectionService,
                name + "-mariadb-source", ConnectorType.MARIADB, managedUrl, connectionParams, credentials);
        var sinkId = ManagedDataSourceTestSupport.insert(dataSources, protectionService,
                name + "-mariadb-sink", ConnectorType.MARIADB, managedUrl, connectionParams, credentials);
        var job = jobDefinitions.insert(JobDefinitionTestFixtures.aJobDefinition()
                .withName(name + "-mariadb-replication")
                .withSourceDatasourceId(sourceId)
                .withSourceTable("replica_source")
                .withSinkDatasourceId(sinkId)
                .withSinkTable("replica_sink")
                .build());
        var pending = jobRuns.insertPendingNow(job.id(), null, 1);

        var outcome = executionService.executeNextPending("mariadb-integration-worker").orElseThrow();
        var persistedRun = jobRuns.findById(pending.id()).orElseThrow();
        assertEquals(pending.id(), outcome.runId());
        assertEquals(JobRunStatus.SUCCEEDED, persistedRun.status(), persistedRun::errorMessage);
        assertEquals(1, outcome.rowsProcessed());

        try (var connection = DriverManager.getConnection(fixtureUrl, username, password);
             var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT id, payload FROM replica_sink")) {
            assertTrue(rows.next());
            assertEquals(1, rows.getInt("id"));
            assertEquals("replicated row", rows.getString("payload"));
            assertFalse(rows.next());
        }
    }
}
