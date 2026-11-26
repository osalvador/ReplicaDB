package org.replicadb.mariadb;

import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MariaDBTlsTest {
    @Container
    private static final MariaDBContainer<?> MARIADB = new MariaDBContainer<>("mariadb:11.4")
            .withCommand("--require-secure-transport=ON", "--local-infile=1");

    @Test
    void replicatesFromAndToMariaDbUsingTheDriversDefaultTls() throws Exception {
        String url = MARIADB.getJdbcUrl();
        String username = MARIADB.getUsername();
        String password = MARIADB.getPassword();
        assertFalse(url.contains("sslMode"), "The test must use the driver's default TLS mode");

        try (var connection = DriverManager.getConnection(url, username, password);
             var statement = connection.createStatement()) {
            try (var status = statement.executeQuery("SHOW SESSION STATUS LIKE 'Ssl_cipher'")) {
                assertTrue(status.next());
                assertFalse(status.getString(2).isBlank(), "The JDBC session must negotiate TLS");
            }

            statement.execute("CREATE TABLE tls_source (id INT PRIMARY KEY, payload VARCHAR(30))");
            statement.execute("CREATE TABLE tls_sink (id INT PRIMARY KEY, payload VARCHAR(30))");
            statement.execute("INSERT INTO tls_source VALUES (1, 'secure row')");
        }

        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", url, "--source-user", username, "--source-password", password,
                "--source-table", "tls_source",
                "--sink-connect", url, "--sink-user", username, "--sink-password", password,
                "--sink-table", "tls_sink"
        });
        assertEquals(0, ReplicaDB.processReplica(options));

        try (var connection = DriverManager.getConnection(url, username, password);
             var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT id, payload FROM tls_sink")) {
            assertTrue(rows.next());
            assertEquals(1, rows.getInt("id"));
            assertEquals("secure row", rows.getString("payload"));
            assertFalse(rows.next());
        }
    }
}
