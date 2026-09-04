package org.replicadb.server.local;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class EmbeddedPostgresLaunchOptions {

    private final String[] arguments;
    private final Properties properties;
    private final Map<String, String> environment;
    private final EmbeddedPostgresProperties embeddedPostgresProperties;

    private EmbeddedPostgresLaunchOptions(String[] arguments, Properties properties,
                                          Map<String, String> environment,
                                          EmbeddedPostgresProperties embeddedPostgresProperties) {
        this.arguments = arguments.clone();
        this.properties = copy(properties);
        this.environment = Map.copyOf(environment);
        this.embeddedPostgresProperties = embeddedPostgresProperties;
    }

    public static EmbeddedPostgresLaunchOptions resolve(String[] arguments, Properties systemProperties,
                                                        Map<String, String> environment) {
        Objects.requireNonNull(arguments, "arguments must not be null");
        Objects.requireNonNull(systemProperties, "systemProperties must not be null");
        Objects.requireNonNull(environment, "environment must not be null");
        Properties mergedProperties = copy(systemProperties);
        for (String argument : arguments) {
            addCommandLineProperty(mergedProperties, argument);
        }
        EmbeddedPostgresProperties embeddedProperties = EmbeddedPostgresProperties.resolve(mergedProperties,
                environment);
        if (embeddedProperties.isEnabled()) {
            validateProfiles(mergedProperties, environment);
            validateLocalExecution(mergedProperties, environment);
        }
        return new EmbeddedPostgresLaunchOptions(arguments, mergedProperties, environment, embeddedProperties);
    }

    public boolean isEmbeddedPostgresEnabled() {
        return embeddedPostgresProperties.isEnabled();
    }

    public EmbeddedPostgresProperties getEmbeddedPostgresProperties() {
        return embeddedPostgresProperties;
    }

    public String[] getArguments() {
        return arguments.clone();
    }

    public Properties getSpringDefaults(EmbeddedPostgresRuntime runtime, Path keyringPath) {
        return getSpringDefaults(runtime, keyringPath, Map.of());
    }

    public Properties getSpringDefaults(EmbeddedPostgresRuntime runtime, Path keyringPath,
                                        Map<String, String> environment) {
        Objects.requireNonNull(runtime, "runtime must not be null");
        Objects.requireNonNull(keyringPath, "keyringPath must not be null");
        Objects.requireNonNull(environment, "environment must not be null");
        Properties defaults = new Properties();
        defaults.setProperty("DB_URL", runtime.getJdbcUrl());
        defaults.setProperty("DB_USERNAME", runtime.getUsername());
        defaults.setProperty("DB_PASSWORD", runtime.getPassword());
        defaults.setProperty("spring.datasource.url", runtime.getJdbcUrl());
        defaults.setProperty("spring.datasource.username", runtime.getUsername());
        defaults.setProperty("spring.datasource.password", runtime.getPassword());
        defaults.setProperty("replicadb.security.bootstrap.enabled", "true");
        defaults.setProperty("REPLICADB_SECURITY_BOOTSTRAP_ENABLED", "true");
        defaults.setProperty("replicadb.server.local-execution.enabled", "true");
        defaults.setProperty("REPLICADB_SECURITY_MASTER_KEY_FILE", keyringPath.toAbsolutePath().toString());
        defaults.setProperty("replicadb.security.master-key-file", keyringPath.toAbsolutePath().toString());
        copyIfPresent(defaults, "REPLICADB_BOOTSTRAP_ADMIN_USERNAME", environment);
        copyIfPresent(defaults, "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD", environment);
        return defaults;
    }

    public Properties getResolvedProperties() {
        return copy(properties);
    }

    private static void addCommandLineProperty(Properties properties, String argument) {
        if (argument == null || !argument.startsWith("--") || "--".equals(argument)) {
            return;
        }
        String property = argument.substring(2);
        int separator = property.indexOf('=');
        if (separator < 0) {
            properties.setProperty(property, "true");
            return;
        }
        if (separator == 0) {
            return;
        }
        String propertyName = property.substring(0, separator);
        if ("REPLICADB_BOOTSTRAP_ADMIN_PASSWORD".equals(propertyName)
            || "replicadb.security.bootstrap.password".equals(propertyName)) {
            throw new IllegalArgumentException(
                "Bootstrap administrator passwords must be supplied through the environment or a secret manager");
        }
        properties.setProperty(propertyName, property.substring(separator + 1));
    }

    private static void validateProfiles(Properties properties, Map<String, String> environment) {
        String activeProfiles = value(properties, environment, "spring.profiles.active", "SPRING_PROFILES_ACTIVE");
        if (activeProfiles != null && containsProfile(activeProfiles, "worker")) {
            throw new IllegalArgumentException(
                    "Embedded PostgreSQL mode cannot be combined with the worker Spring profile");
        }
    }

    private static void validateLocalExecution(Properties properties, Map<String, String> environment) {
        String localExecution = value(properties, environment, "replicadb.server.local-execution.enabled",
                "REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED");
        if ("false".equalsIgnoreCase(localExecution)) {
            throw new IllegalArgumentException(
                    "Embedded PostgreSQL mode requires replicadb.server.local-execution.enabled=true");
        }
    }

    private static boolean containsProfile(String profiles, String expected) {
        for (String profile : profiles.split(",")) {
            if (expected.equalsIgnoreCase(profile.trim())) {
                return true;
            }
        }
        return false;
    }

    private static String value(Properties properties, Map<String, String> environment,
                                String property, String environmentKey) {
        String propertyValue = properties.getProperty(property);
        return propertyValue == null ? environment.get(environmentKey) : propertyValue;
    }

    private static Properties copy(Properties source) {
        Properties copy = new Properties();
        for (String name : source.stringPropertyNames()) {
            copy.setProperty(name, source.getProperty(name));
        }
        return copy;
    }

    private void copyIfPresent(Properties defaults, String property, Map<String, String> environment) {
        String value = properties.getProperty(property);
        if (value == null) {
            value = environment.get(property);
        }
        if (value != null) {
            defaults.setProperty(property, value);
        }
    }
}
