package org.replicadb.server.job.domain;

import org.replicadb.config.CredentialRedactor;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.Locale;
import java.util.regex.Pattern;

public final class DataSourceSecurityKeyPolicy {

    private static final Pattern SENSITIVE_KEY = Pattern.compile(
            "(?i)(password|passwd|pwd|secret|token|credential|accesskey|secretkey|sasl|keystore|truststore|privatekey|certificate|clientkey)");

    private DataSourceSecurityKeyPolicy() {
    }

    public static void validateTechnicalParameters(Map<String, String> technicalParams) {
        if (technicalParams == null) {
            return;
        }
        technicalParams.forEach((key, value) -> {
            requireKey(key);
            if (isSensitiveTechnicalKey(key) || looksLikeCredentialValue(value)) {
                throw new IllegalArgumentException(
                        "technicalParams must not contain credential-like values: " + key);
            }
        });
    }

    public static void validateSecurityParameters(Map<String, String> securityParameters) {
        if (securityParameters == null) {
            return;
        }
        securityParameters.forEach((key, value) -> {
            requireKey(key);
            if (!isAllowedSecurityKey(key)) {
                throw new IllegalArgumentException("Unsupported datasource security key: " + key);
            }
        });
    }

    public static Map<String, String> mergeSecurityParameters(Map<String, String> existing,
                                                               Map<String, String> requested,
                                                               Set<String> clearSecurityKeys) {
        Map<String, String> current = existing == null ? Map.of() : existing;
        Set<String> clear = clearSecurityKeys == null ? Set.of() : clearSecurityKeys;
        validateSecurityParameters(current);
        current.forEach((key, value) -> {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Existing datasource security values must not be blank: " + key);
            }
        });
        validateSecurityParameters(requested);
        clear.forEach(DataSourceSecurityKeyPolicy::validateClearKey);

        TreeMap<String, String> merged = new TreeMap<>(current);
        clear.forEach(merged::remove);
        if (requested != null) {
            requested.forEach((key, value) -> {
                if (value != null && !value.isBlank()) {
                    merged.put(key, value);
                }
            });
        }
        if (!hasValue(merged.get("connect"))) {
            throw new IllegalArgumentException("Datasource security must contain connect");
        }
        return Map.copyOf(merged);
    }

    public static boolean isSecurityKey(String key) {
        if (key == null) {
            return false;
        }
        String normalized = key.replaceAll("[^A-Za-z0-9.]", "").toLowerCase(Locale.ROOT);
        return normalized.equals("connect")
                || normalized.equals("user")
                || normalized.equals("password")
                || normalized.startsWith("auth.")
                || (normalized.startsWith("connect.parameter.")
                && SENSITIVE_KEY.matcher(normalized.substring("connect.parameter.".length())).find());
    }

    public static boolean isSensitiveTechnicalKey(String key) {
        if (key == null) {
            return false;
        }
        String normalized = key.replaceAll("[^A-Za-z0-9.]", "").toLowerCase(Locale.ROOT);
        return SENSITIVE_KEY.matcher(normalized).find();
    }

    private static boolean isAllowedSecurityKey(String key) {
        return key.equals("connect") || key.equals("user") || key.equals("password")
                || key.startsWith("auth.") || key.startsWith("connect.parameter.");
    }

    private static void validateClearKey(String key) {
        requireKey(key);
        if (!isAllowedSecurityKey(key)) {
            throw new IllegalArgumentException("Unsupported datasource security key: " + key);
        }
        if ("connect".equals(key)) {
            throw new IllegalArgumentException("Datasource connect cannot be cleared");
        }
    }

    private static boolean looksLikeCredentialValue(String value) {
        return value != null && !value.isBlank()
                && !value.equals(CredentialRedactor.redactConnectionString(value));
    }

    private static void requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Datasource security key must not be blank");
        }
    }

    private static boolean hasValue(String value) {
        return value != null && !value.isBlank();
    }
}
