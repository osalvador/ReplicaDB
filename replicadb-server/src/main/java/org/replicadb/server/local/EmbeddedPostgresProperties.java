package org.replicadb.server.local;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class EmbeddedPostgresProperties {

    public static final String ENABLED_PROPERTY = "replicadb.embedded-postgres.enabled";
    public static final String HOME_PROPERTY = "replicadb.server.home";
    public static final String VERSION_PROPERTY = "replicadb.embedded-postgres.version";
    public static final String PORT_PROPERTY = "replicadb.embedded-postgres.port";
    public static final String STARTUP_TIMEOUT_PROPERTY = "replicadb.embedded-postgres.startup-timeout";
    public static final String DOWNLOAD_TIMEOUT_PROPERTY = "replicadb.embedded-postgres.download-timeout";
    public static final String DOWNLOAD_RETRIES_PROPERTY = "replicadb.embedded-postgres.download-retries";

    public static final String ENABLED_ENVIRONMENT = "REPLICADB_EMBEDDED_POSTGRES_ENABLED";
    public static final String HOME_ENVIRONMENT = "REPLICADB_SERVER_HOME";
    public static final String LEGACY_HOME_ENVIRONMENT = "REPLICADB_HOME";
    public static final String VERSION_ENVIRONMENT = "REPLICADB_EMBEDDED_POSTGRES_VERSION";
    public static final String PORT_ENVIRONMENT = "REPLICADB_EMBEDDED_POSTGRES_PORT";
    public static final String STARTUP_TIMEOUT_ENVIRONMENT = "REPLICADB_EMBEDDED_POSTGRES_STARTUP_TIMEOUT";
    public static final String DOWNLOAD_TIMEOUT_ENVIRONMENT = "REPLICADB_EMBEDDED_POSTGRES_DOWNLOAD_TIMEOUT";
    public static final String DOWNLOAD_RETRIES_ENVIRONMENT = "REPLICADB_EMBEDDED_POSTGRES_DOWNLOAD_RETRIES";

    private static final String DEFAULT_VERSION = "14.22.0";
    private static final int DEFAULT_PORT = 0;
    private static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration DEFAULT_DOWNLOAD_TIMEOUT = Duration.ofMinutes(2);
    private static final int DEFAULT_DOWNLOAD_RETRIES = 3;

    private final boolean enabled;
    private final EmbeddedPostgresHome home;
    private final String postgresVersion;
    private final int port;
    private final Duration startupTimeout;
    private final Duration downloadTimeout;
    private final int downloadRetries;

    private EmbeddedPostgresProperties(boolean enabled, EmbeddedPostgresHome home, String postgresVersion,
                                      int port, Duration startupTimeout, Duration downloadTimeout,
                                      int downloadRetries) {
        this.enabled = enabled;
        this.home = Objects.requireNonNull(home, "home must not be null");
        this.postgresVersion = requireText(postgresVersion, VERSION_PROPERTY);
        this.port = port;
        this.startupTimeout = requirePositive(startupTimeout, STARTUP_TIMEOUT_PROPERTY);
        this.downloadTimeout = requirePositive(downloadTimeout, DOWNLOAD_TIMEOUT_PROPERTY);
        if (downloadRetries < 0) {
            throw new IllegalArgumentException(DOWNLOAD_RETRIES_PROPERTY + " must not be negative");
        }
        this.downloadRetries = downloadRetries;
    }

    public static EmbeddedPostgresProperties resolve(Properties systemProperties,
                                                     Map<String, String> environment) {
        Objects.requireNonNull(systemProperties, "systemProperties must not be null");
        Objects.requireNonNull(environment, "environment must not be null");

        rejectLegacyCliHome(environment);

        boolean enabled = booleanValue(systemProperties, environment, ENABLED_PROPERTY, ENABLED_ENVIRONMENT, false);
        String configuredHome = value(systemProperties, environment, HOME_PROPERTY, HOME_ENVIRONMENT);
        Path homePath = configuredHome == null
                ? defaultHome(systemProperties)
                : Path.of(configuredHome);
        EmbeddedPostgresHome home = EmbeddedPostgresHome.from(homePath);
        String version = valueOrDefault(systemProperties, environment, VERSION_PROPERTY, VERSION_ENVIRONMENT,
                DEFAULT_VERSION);
        int port = integerValue(systemProperties, environment, PORT_PROPERTY, PORT_ENVIRONMENT, DEFAULT_PORT);
        Duration startupTimeout = durationValue(systemProperties, environment, STARTUP_TIMEOUT_PROPERTY,
                STARTUP_TIMEOUT_ENVIRONMENT, DEFAULT_STARTUP_TIMEOUT);
        Duration downloadTimeout = durationValue(systemProperties, environment, DOWNLOAD_TIMEOUT_PROPERTY,
                DOWNLOAD_TIMEOUT_ENVIRONMENT, DEFAULT_DOWNLOAD_TIMEOUT);
        int downloadRetries = integerValue(systemProperties, environment, DOWNLOAD_RETRIES_PROPERTY,
                DOWNLOAD_RETRIES_ENVIRONMENT, DEFAULT_DOWNLOAD_RETRIES);

        if (enabled) {
            validateNoExternalDatasourceConfiguration(systemProperties, environment);
        }
        return new EmbeddedPostgresProperties(enabled, home, version, port, startupTimeout, downloadTimeout,
                downloadRetries);
    }

    public static void validateNoExternalDatasourceConfiguration(Properties systemProperties,
                                                                  Map<String, String> environment) {
        Objects.requireNonNull(systemProperties, "systemProperties must not be null");
        Objects.requireNonNull(environment, "environment must not be null");

        List<String> configuredProperties = new ArrayList<>();
        addIfPresent(configuredProperties, systemProperties, environment, "DB_URL", "SPRING_DATASOURCE_URL");
        addIfPresent(configuredProperties, systemProperties, environment, "DB_USERNAME", "SPRING_DATASOURCE_USERNAME");
        addIfPresent(configuredProperties, systemProperties, environment, "DB_PASSWORD", "SPRING_DATASOURCE_PASSWORD");
        addIfPresent(configuredProperties, systemProperties, environment, "spring.datasource.url", null);
        addIfPresent(configuredProperties, systemProperties, environment, "spring.datasource.username", null);
        addIfPresent(configuredProperties, systemProperties, environment, "spring.datasource.password", null);
        if (!configuredProperties.isEmpty()) {
            throw new IllegalArgumentException(
                    "Embedded PostgreSQL cannot be combined with external metadata configuration: "
                            + String.join(", ", configuredProperties));
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public EmbeddedPostgresHome getHome() {
        return home;
    }

    public String getPostgresVersion() {
        return postgresVersion;
    }

    public int getPort() {
        return port;
    }

    public Duration getStartupTimeout() {
        return startupTimeout;
    }

    public Duration getDownloadTimeout() {
        return downloadTimeout;
    }

    public int getDownloadRetries() {
        return downloadRetries;
    }

    private static Path defaultHome(Properties systemProperties) {
        String userHome = systemProperties.getProperty("user.home");
        if (userHome == null || userHome.isBlank()) {
            throw new IllegalArgumentException("user.home must be configured when " + HOME_PROPERTY + " is absent");
        }
        return Path.of(userHome, ".replicadb");
    }

    private static void rejectLegacyCliHome(Map<String, String> environment) {
        String legacyHome = environment.get(LEGACY_HOME_ENVIRONMENT);
        if (legacyHome != null && !legacyHome.isBlank()) {
            throw new IllegalArgumentException(
                    LEGACY_HOME_ENVIRONMENT + " is reserved for the CLI; configure "
                            + HOME_ENVIRONMENT + " for server state");
        }
    }

    private static void addIfPresent(List<String> configuredProperties, Properties systemProperties,
                                     Map<String, String> environment, String systemKey, String environmentKey) {
        if (systemProperties.containsKey(systemKey)
                || environment.containsKey(systemKey)
                || (environmentKey != null && environment.containsKey(environmentKey))) {
            configuredProperties.add(systemKey);
        }
    }

    private static boolean booleanValue(Properties systemProperties, Map<String, String> environment,
                                        String systemKey, String environmentKey, boolean defaultValue) {
        String value = value(systemProperties, environment, systemKey, environmentKey);
        if (value == null) {
            return defaultValue;
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException(systemKey + " must be true or false");
        }
        return Boolean.parseBoolean(value);
    }

    private static int integerValue(Properties systemProperties, Map<String, String> environment,
                                    String systemKey, String environmentKey, int defaultValue) {
        String value = value(systemProperties, environment, systemKey, environmentKey);
        if (value == null) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (PORT_PROPERTY.equals(systemKey) && (parsed < 0 || parsed > 65_535)) {
                throw new IllegalArgumentException(systemKey + " must be between 0 and 65535");
            }
            if (DOWNLOAD_RETRIES_PROPERTY.equals(systemKey) && parsed < 0) {
                throw new IllegalArgumentException(systemKey + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(systemKey + " must be an integer", exception);
        }
    }

    private static Duration durationValue(Properties systemProperties, Map<String, String> environment,
                                          String systemKey, String environmentKey, Duration defaultValue) {
        String value = value(systemProperties, environment, systemKey, environmentKey);
        if (value == null) {
            return defaultValue;
        }
        try {
            if (value.startsWith("PT")) {
                return requirePositive(Duration.parse(value), systemKey);
            }
            if (value.endsWith("ms")) {
                return requirePositive(Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2))),
                        systemKey);
            }
            if (value.endsWith("s")) {
                return requirePositive(Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1))),
                        systemKey);
            }
            if (value.endsWith("m")) {
                return requirePositive(Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1))),
                        systemKey);
            }
            return requirePositive(Duration.ofSeconds(Long.parseLong(value)), systemKey);
        } catch (NumberFormatException | java.time.format.DateTimeParseException exception) {
            throw new IllegalArgumentException(systemKey + " must be a positive duration", exception);
        }
    }

    private static String valueOrDefault(Properties systemProperties, Map<String, String> environment,
                                         String systemKey, String environmentKey, String defaultValue) {
        String value = value(systemProperties, environment, systemKey, environmentKey);
        return value == null ? defaultValue : value;
    }

    private static String value(Properties systemProperties, Map<String, String> environment,
                                String systemKey, String environmentKey) {
        String systemValue = systemProperties.getProperty(systemKey);
        if (systemValue != null) {
            return systemValue;
        }
        return environment.get(environmentKey);
    }

    private static String requireText(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(property + " must not be blank");
        }
        return value;
    }

    private static Duration requirePositive(Duration value, String property) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(property + " must be positive");
        }
        return value;
    }
}
