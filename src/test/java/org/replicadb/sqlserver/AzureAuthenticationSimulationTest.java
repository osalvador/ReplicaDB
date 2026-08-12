package org.replicadb.sqlserver;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.AzureAuthenticationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.SQLServerManager;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies the ReplicaDB-to-JDBC authentication boundary without Azure or network access.
 */
class AzureAuthenticationSimulationTest {

    @Test
    void passesAllFirstClassModesToTheJdbcDriverForSourceAndSink() throws Exception {
        List<AzureAuthenticationMode> modes = List.of(
                AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE,
                AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT,
                AzureAuthenticationMode.ACTIVE_DIRECTORY_MANAGED_IDENTITY,
                AzureAuthenticationMode.ACTIVE_DIRECTORY_SERVICE_PRINCIPAL,
                AzureAuthenticationMode.ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE
        );

        for (AzureAuthenticationMode mode : modes) {
            for (DataSourceType dataSourceType : DataSourceType.values()) {
                CapturingAzureDriver.reset();
                ToolOptions options = options(dataSourceType);
                configure(options, dataSourceType, mode);

                SimulatedSQLServerManager manager = new SimulatedSQLServerManager(options, dataSourceType);
                Connection connection = manager.getConnection();
                Properties properties = CapturingAzureDriver.lastProperties;

                assertNotNull(connection);
                assertNotNull(properties);
                assertEquals(mode.getDriverValue(), properties.getProperty("authentication"));
                assertFalse(connection.isClosed());
                assertModeProperties(mode, properties);

                manager.close();
                assertTrueClosed(connection);
            }
        }

        assertEquals(modes.size() * DataSourceType.values().length, CapturingAzureDriver.connectionCount);
    }

    private void assertModeProperties(AzureAuthenticationMode mode, Properties properties) {
        switch (mode) {
            case ACTIVE_DIRECTORY_INTERACTIVE:
                assertEquals("operator@example.com", properties.getProperty("user"));
                assertNull(properties.getProperty("password"));
                break;
            case ACTIVE_DIRECTORY_DEFAULT:
                assertNull(properties.getProperty("user"));
                assertNull(properties.getProperty("password"));
                break;
            case ACTIVE_DIRECTORY_MANAGED_IDENTITY:
                assertEquals("managed-identity-client", properties.getProperty("msiClientId"));
                assertNull(properties.getProperty("password"));
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL:
                assertEquals("service-principal-client", properties.getProperty("user"));
                assertEquals("simulated-secret", properties.getProperty("password"));
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE:
                assertEquals("certificate-client", properties.getProperty("user"));
                assertEquals("${AZURE_CLIENT_CERTIFICATE}", properties.getProperty("clientCertificate"));
                assertEquals("${AZURE_CLIENT_KEY}", properties.getProperty("clientKey"));
                assertNull(properties.getProperty("password"));
                break;
            default:
                throw new AssertionError("Unexpected mode: " + mode);
        }
    }

    private void assertTrueClosed(Connection connection) throws SQLException {
        org.junit.jupiter.api.Assertions.assertTrue(connection.isClosed());
    }

    private ToolOptions options(DataSourceType dataSourceType) throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:azure-sim:source",
                "--sink-connect", "jdbc:azure-sim:sink",
                "--jobs", "1"
        });
    }

    private void configure(ToolOptions options, DataSourceType dataSourceType, AzureAuthenticationMode mode) {
        if (DataSourceType.SOURCE.equals(dataSourceType)) {
            options.setSourceAuthMode(mode.toString());
            configureSource(options, mode);
        } else {
            options.setSinkAuthMode(mode.toString());
            configureSink(options, mode);
        }
    }

    private void configureSource(ToolOptions options, AzureAuthenticationMode mode) {
        switch (mode) {
            case ACTIVE_DIRECTORY_INTERACTIVE:
                options.setSourceAuthLoginHint("operator@example.com");
                break;
            case ACTIVE_DIRECTORY_MANAGED_IDENTITY:
                options.setSourceAuthPrincipalId("managed-identity-client");
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL:
                options.setSourceAuthPrincipalId("service-principal-client");
                options.setSourcePassword("simulated-secret");
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE:
                options.setSourceAuthPrincipalId("certificate-client");
                options.setSourceAuthClientCertificate("${AZURE_CLIENT_CERTIFICATE}");
                options.setSourceAuthClientKey("${AZURE_CLIENT_KEY}");
                break;
            default:
                break;
        }
    }

    private void configureSink(ToolOptions options, AzureAuthenticationMode mode) {
        switch (mode) {
            case ACTIVE_DIRECTORY_INTERACTIVE:
                options.setSinkAuthLoginHint("operator@example.com");
                break;
            case ACTIVE_DIRECTORY_MANAGED_IDENTITY:
                options.setSinkAuthPrincipalId("managed-identity-client");
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL:
                options.setSinkAuthPrincipalId("service-principal-client");
                options.setSinkPassword("simulated-secret");
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE:
                options.setSinkAuthPrincipalId("certificate-client");
                options.setSinkAuthClientCertificate("${AZURE_CLIENT_CERTIFICATE}");
                options.setSinkAuthClientKey("${AZURE_CLIENT_KEY}");
                break;
            default:
                break;
        }
    }

    private static final class SimulatedSQLServerManager extends SQLServerManager {
        private SimulatedSQLServerManager(ToolOptions options, DataSourceType dataSourceType) {
            super(options, dataSourceType);
        }

        @Override
        public String getDriverClass() {
            return CapturingAzureDriver.class.getName();
        }
    }

    public static final class CapturingAzureDriver implements Driver {
        private static final String URL_PREFIX = "jdbc:azure-sim:";
        private static Properties lastProperties;
        private static int connectionCount;

        static {
            try {
                DriverManager.registerDriver(new CapturingAzureDriver());
            } catch (SQLException exception) {
                throw new ExceptionInInitializerError(exception);
            }
        }

        private static void reset() {
            lastProperties = null;
        }

        @Override
        public Connection connect(String url, Properties properties) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            lastProperties = new Properties();
            lastProperties.putAll(properties);
            connectionCount++;
            return connectionProxy();
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(URL_PREFIX);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return Logger.getGlobal();
        }

        private Connection connectionProxy() {
            class State {
                private boolean autoCommit = true;
                private boolean closed;
            }
            State state = new State();
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "setAutoCommit" -> {
                            state.autoCommit = (Boolean) args[0];
                            yield null;
                        }
                        case "getAutoCommit" -> state.autoCommit;
                        case "close" -> {
                            state.closed = true;
                            yield null;
                        }
                        case "isClosed" -> state.closed;
                        case "rollback", "commit" -> null;
                        case "toString" -> "simulated-azure-connection";
                        default -> defaultValue(method.getReturnType());
                    });
        }

        private Object defaultValue(Class<?> returnType) {
            if (!returnType.isPrimitive()) {
                return null;
            }
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == byte.class) {
                return (byte) 0;
            }
            if (returnType == short.class) {
                return (short) 0;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            if (returnType == float.class) {
                return 0F;
            }
            if (returnType == double.class) {
                return 0D;
            }
            if (returnType == char.class) {
                return '\0';
            }
            return null;
        }
    }
}
