package org.replicadb.server.job.domain;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public record ConnectionCredentials(
        String connect,
        String user,
        String password,
        AzureAuthentication authentication,
        /**
         * Reserved keys include format, format.delimiter, format.quote, format.escape, format.nullString,
         * format.firstRecordAsHeader, format.ignoreEmptyLines, format.ignoreSurroundingSpaces, format.trim,
         * format.recordSeparator, topic, partition, and acks.
         */
        Map<String, String> connectionParams) {

    private static final Pattern ENV_REFERENCE = Pattern.compile("\\$\\{env:[A-Za-z_][A-Za-z0-9_]*}");
    private static final Pattern EMBEDDED_CREDENTIAL = Pattern.compile(
            "(?i)(?:password|passwd|pwd|secret|token)\\s*=|://[^/?#;]+:[^/?#;]+@");
    private static final Pattern SENSITIVE_PARAMETER = Pattern.compile(
            "(?i)(?:password|passwd|pwd|secret|token|client[_-]?key|private[_-]?key|certificate)");

    public ConnectionCredentials {
        requireNonBlank("connect", connect);
        if (EMBEDDED_CREDENTIAL.matcher(connect).find()) {
            throw new IllegalArgumentException("connect must not contain embedded credentials");
        }
        if (password != null && !ENV_REFERENCE.matcher(password).matches()) {
            throw new IllegalArgumentException("password must be an ${env:VARIABLE} reference");
        }
        authentication = authentication == null
                ? new AzureAuthentication(null, null, null, null, null)
                : authentication;
        connectionParams = connectionParams == null ? Map.of() : Map.copyOf(connectionParams);
        for (Map.Entry<String, String> entry : connectionParams.entrySet()) {
            if (SENSITIVE_PARAMETER.matcher(entry.getKey()).find()
                    || SENSITIVE_PARAMETER.matcher(Objects.toString(entry.getValue(), "")).find()) {
                throw new IllegalArgumentException("connectionParams must not contain credential-like values");
            }
        }
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
