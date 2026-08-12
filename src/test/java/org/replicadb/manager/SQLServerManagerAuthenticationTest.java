package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.AzureAuthenticationMode;
import org.replicadb.cli.ToolOptions;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SQLServerManagerAuthenticationTest {

    @Test
    void mapsInteractiveAuthenticationAndLoginHint() throws Exception {
        ToolOptions options = options();
        options.setSourceUser("user@example.com");
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE);
        options.getSourceAuthentication().setLoginHint("user@example.com");

        Properties properties = manager(options).properties(DataSourceType.SOURCE);

        assertEquals("ActiveDirectoryInteractive", properties.getProperty("authentication"));
        assertEquals("user@example.com", properties.getProperty("user"));
        assertNull(properties.getProperty("password"));
    }

    @Test
    void mapsDefaultAndManagedIdentityClientIds() throws Exception {
        ToolOptions options = options();
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT);
        options.getSourceAuthentication().setPrincipalId("default-client");
        Properties defaultProperties = manager(options).properties(DataSourceType.SOURCE);

        options.getSinkAuthentication().setMode(AzureAuthenticationMode.fromValue("ActiveDirectoryMSI"));
        options.getSinkAuthentication().setPrincipalId("managed-client");
        Properties managedProperties = manager(options).properties(DataSourceType.SINK);

        assertEquals("ActiveDirectoryDefault", defaultProperties.getProperty("authentication"));
        assertEquals("default-client", defaultProperties.getProperty("msiClientId"));
        assertEquals("ActiveDirectoryManagedIdentity", managedProperties.getProperty("authentication"));
        assertEquals("managed-client", managedProperties.getProperty("msiClientId"));
    }

    @Test
    void mapsServicePrincipalSecretAndCertificateProperties() throws Exception {
        ToolOptions options = options();
        options.setSourcePassword("service-secret");
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_SERVICE_PRINCIPAL);
        options.getSourceAuthentication().setPrincipalId("service-client");
        Properties serviceProperties = manager(options).properties(DataSourceType.SOURCE);

        options.setSinkPassword("certificate-password");
        options.getSinkAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE);
        options.getSinkAuthentication().setPrincipalId("certificate-client");
        options.getSinkAuthentication().setClientCertificate("/tmp/client.pfx");
        options.getSinkAuthentication().setClientKey("/tmp/client.key");
        Properties certificateProperties = manager(options).properties(DataSourceType.SINK);

        assertEquals("ActiveDirectoryServicePrincipal", serviceProperties.getProperty("authentication"));
        assertEquals("service-client", serviceProperties.getProperty("user"));
        assertEquals("service-secret", serviceProperties.getProperty("password"));
        assertEquals("ActiveDirectoryServicePrincipalCertificate", certificateProperties.getProperty("authentication"));
        assertEquals("certificate-client", certificateProperties.getProperty("user"));
        assertEquals("/tmp/client.pfx", certificateProperties.getProperty("clientCertificate"));
        assertEquals("/tmp/client.key", certificateProperties.getProperty("clientKey"));
        assertEquals("certificate-password", certificateProperties.getProperty("password"));
    }

    @Test
    void removesCredentialsForIntegratedAuthentication() throws Exception {
        ToolOptions options = options();
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTEGRATED);

        Properties properties = manager(options).properties(DataSourceType.SOURCE);

        assertEquals("ActiveDirectoryIntegrated", properties.getProperty("authentication"));
        assertNull(properties.getProperty("user"));
        assertNull(properties.getProperty("password"));
    }

    @Test
    void rejectsConflictingAuthenticationProperties() throws Exception {
        ToolOptions options = options();
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE);
        Properties connectionParams = new Properties();
        connectionParams.setProperty("authentication", "ActiveDirectoryDefault");
        options.setSourceConnectionParams(connectionParams);

        assertThrows(SQLException.class, () -> manager(options).properties(DataSourceType.SOURCE));
    }

    @Test
    void preservesRawJdbcParametersAndOrdinaryConnectionPath() throws Exception {
        ToolOptions options = options();
        Properties connectionParams = new Properties();
        connectionParams.setProperty("authentication", "ActiveDirectoryInteractive");
        options.setSourceConnectionParams(connectionParams);

        ExposedSQLServerManager manager = manager(options);
        Properties rawProperties = manager.properties(DataSourceType.SOURCE);

        assertEquals("ActiveDirectoryInteractive", rawProperties.getProperty("authentication"));

        options.setSourceConnectionParams(null);
        assertNull(manager.build(DataSourceType.SOURCE));
    }

    @Test
    void rejectsUserWithIntegratedAuthentication() throws Exception {
        ToolOptions options = options();
        options.setSourceUser("domain-user");
        options.getSourceAuthentication().setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTEGRATED);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> manager(options).properties(DataSourceType.SOURCE));
        assertTrue(exception.getMessage().contains("User"));
    }

    private ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlserver://source",
                "--sink-connect", "jdbc:sqlserver://sink"
        });
    }

    private ExposedSQLServerManager manager(ToolOptions options) {
        return new ExposedSQLServerManager(options);
    }

    private static final class ExposedSQLServerManager extends SQLServerManager {
        private ExposedSQLServerManager(ToolOptions options) {
            super(options, DataSourceType.SOURCE);
        }

        private Properties properties(DataSourceType dataSourceType) throws SQLException {
            Properties properties = buildConnectionProperties(dataSourceType);
            if (properties == null) {
                properties = new Properties();
            }
            customizeConnectionProperties(dataSourceType, properties);
            return properties;
        }

        private Properties build(DataSourceType dataSourceType) {
            return buildConnectionProperties(dataSourceType);
        }
    }
}
