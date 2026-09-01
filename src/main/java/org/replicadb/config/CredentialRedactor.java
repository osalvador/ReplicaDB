package org.replicadb.config;

import io.sentry.Breadcrumb;
import io.sentry.SentryEvent;
import io.sentry.protocol.Message;
import io.sentry.protocol.SentryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CredentialRedactor {

    public static final String REDACTED_VALUE = "[REDACTED]";

    private static final Set<String> SECRET_KEYS = new HashSet<>(Arrays.asList(
            "password",
            "accesstoken",
            "clientsecret",
            "aadsecureprincipalsecret",
            "clientkeypassword",
            "privatekeypassword",
            "secretkey",
            "sastoken",
            "sentrydsn",
            "certificate",
            "clientcertificate"
    ));

    private static final Set<String> IDENTITY_KEYS = new HashSet<>(Arrays.asList(
            "user",
            "username",
            "loginhint",
            "principalid",
            "clientid",
            "tenantid",
            "msiclientid"
    ));

    private static final Pattern URL_USER_INFO = Pattern.compile(
            "(?i)(://)([^/?;:@]+)(:([^@/?;]+))?@");
    private static final Pattern QUERY_PARAMETER = Pattern.compile(
            "(?i)([?;&])(password|access_token|client_secret|refresh_token|sas_token|user|username|login_hint|client_id|tenant_id|msi_client_id)=([^&#;]*)");
    private static final Pattern JDBC_PARAMETER = Pattern.compile(
            "(?i)(^|;)(password|accessToken|clientSecret|AADSecurePrincipalSecret|clientKeyPassword|privateKeyPassword|secretKey|sasToken|user|username|loginHint|clientId|tenantId|msiClientId)=([^;]*)");
    private static final Pattern MESSAGE_PARAMETER = Pattern.compile(
            "(?i)(password|token|accessToken|clientSecret|AADSecurePrincipalSecret|clientKeyPassword|privateKeyPassword|secretKey|sasToken|user|username|loginHint|clientId|tenantId|msiClientId)\\s*=\\s*([^\\s,;]*)");
        private static final Pattern ENVIRONMENT_PLACEHOLDER = Pattern.compile("\\$\\{[^}]+}");
        private static final Pattern PEM_BLOCK = Pattern.compile(
            "(?is)-----BEGIN [^-]+-----.*?-----END [^-]+-----");
        private static final Pattern PEM_MARKER = Pattern.compile("(?i)-----(?:BEGIN|END) [^-]+-----");

    private CredentialRedactor() {
    }

    public static String redactIdentity(String value) {
        return value == null ? null : REDACTED_VALUE;
    }

    public static String redactMessage(String value) {
        if (value == null) {
            return null;
        }
        String redacted = redactConnectionString(value);
        redacted = ENVIRONMENT_PLACEHOLDER.matcher(redacted).replaceAll(REDACTED_VALUE);
        redacted = PEM_BLOCK.matcher(redacted).replaceAll(REDACTED_VALUE);
        return PEM_MARKER.matcher(redacted).replaceAll(REDACTED_VALUE);
    }

    public static String redactConnectionString(String value) {
        if (value == null) {
            return null;
        }

        String redacted = URL_USER_INFO.matcher(value)
                .replaceAll("$1" + REDACTED_VALUE + "@");
        redacted = QUERY_PARAMETER.matcher(redacted)
                .replaceAll("$1$2=" + REDACTED_VALUE);

        Matcher jdbcMatcher = JDBC_PARAMETER.matcher(redacted);
        StringBuffer jdbcResult = new StringBuffer();
        while (jdbcMatcher.find()) {
            jdbcMatcher.appendReplacement(jdbcResult,
                    Matcher.quoteReplacement(jdbcMatcher.group(1) + jdbcMatcher.group(2) + "=" + REDACTED_VALUE));
        }
        jdbcMatcher.appendTail(jdbcResult);
        return MESSAGE_PARAMETER.matcher(jdbcResult.toString())
            .replaceAll("$1=" + REDACTED_VALUE);
    }

    public static Properties redactProperties(Properties source) {
        if (source == null) {
            return null;
        }

        Properties redacted = new Properties();
        for (Map.Entry<Object, Object> entry : source.entrySet()) {
            String key = String.valueOf(entry.getKey());
            redacted.setProperty(key, redactPropertyValue(key, entry.getValue()));
        }
        return redacted;
    }

    public static Object redactObject(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (isSensitiveKey(key)) {
            return REDACTED_VALUE;
        }
        if (value instanceof Properties) {
            return redactProperties((Properties) value);
        }
        if (value instanceof Map<?, ?>) {
            Map<String, Object> redacted = new java.util.HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                String childKey = String.valueOf(entry.getKey());
                redacted.put(childKey, redactObject(childKey, entry.getValue()));
            }
            return redacted;
        }
        if (value instanceof Iterable<?>) {
            List<Object> redacted = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                redacted.add(redactObject(key, item));
            }
            return redacted;
        }
        if (value instanceof String) {
            return redactConnectionString((String) value);
        }
        return value;
    }

    public static SentryEvent redactEvent(SentryEvent event) {
        if (event == null) {
            return null;
        }

        if (event.getMessage() != null) {
            Message message = event.getMessage();
            message.setMessage(redactMessage(message.getMessage()));
            message.setFormatted(redactMessage(message.getFormatted()));
            if (message.getParams() != null) {
                message.setParams(message.getParams().stream()
                        .map(CredentialRedactor::redactMessage)
                        .toList());
            }
        }

        if (event.getTags() != null) {
            Map<String, String> tags = new java.util.HashMap<>();
            for (Map.Entry<String, String> entry : event.getTags().entrySet()) {
                tags.put(entry.getKey(), redactPropertyValue(entry.getKey(), entry.getValue()));
            }
            event.setTags(tags);
        }

        if (event.getContexts() != null) {
            for (Map.Entry<String, Object> entry : event.getContexts().entrySet()) {
                event.getContexts().put(entry.getKey(), redactObject(entry.getKey(), entry.getValue()));
            }
        }

        if (event.getBreadcrumbs() != null) {
            event.setBreadcrumbs(event.getBreadcrumbs().stream()
                    .map(CredentialRedactor::redactBreadcrumb)
                    .toList());
        }

        if (event.getExceptions() != null) {
            for (SentryException exception : event.getExceptions()) {
                exception.setValue(redactMessage(exception.getValue()));
            }
        }

        if (event.getThrowable() != null) {
            event.setThrowable(redactThrowable(event.getThrowable(), new IdentityHashMap<>()));
        }

        return event;
    }

    public static Breadcrumb redactBreadcrumb(Breadcrumb breadcrumb) {
        if (breadcrumb == null) {
            return null;
        }
        breadcrumb.setMessage(redactMessage(breadcrumb.getMessage()));
        if (breadcrumb.getData() != null) {
            for (Map.Entry<String, Object> entry : breadcrumb.getData().entrySet()) {
                breadcrumb.setData(entry.getKey(), redactObject(entry.getKey(), entry.getValue()));
            }
        }
        return breadcrumb;
    }

    public static Throwable redactThrowable(Throwable throwable) {
        return redactThrowable(throwable, new IdentityHashMap<>());
    }

    private static Throwable redactThrowable(Throwable throwable, IdentityHashMap<Throwable, Throwable> seen) {
        if (throwable == null) {
            return null;
        }
        Throwable existing = seen.get(throwable);
        if (existing != null) {
            return existing;
        }

        RuntimeException redacted = new RuntimeException(redactMessage(throwable.getMessage()));
        seen.put(throwable, redacted);
        redacted.setStackTrace(throwable.getStackTrace());
        Throwable cause = throwable.getCause();
        if (cause != null && cause != throwable) {
            redacted.initCause(redactThrowable(cause, seen));
        }
        return redacted;
    }

    private static String redactPropertyValue(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (isSensitiveKey(key)) {
            return REDACTED_VALUE;
        }
        return redactConnectionString(String.valueOf(value));
    }

    private static boolean isSensitiveKey(String key) {
        if (key == null) {
            return false;
        }
        String normalized = key.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
        return SECRET_KEYS.contains(normalized)
                || IDENTITY_KEYS.contains(normalized)
                || normalized.contains("password")
                || normalized.contains("token")
                || normalized.contains("secret")
                || normalized.contains("certificate")
                || normalized.contains("principalid")
                || normalized.contains("loginhint")
                || normalized.contains("clientid")
                || normalized.contains("tenantid")
                || normalized.contains("msiclientid")
                || normalized.endsWith("username")
                || normalized.endsWith("user");
    }
}
