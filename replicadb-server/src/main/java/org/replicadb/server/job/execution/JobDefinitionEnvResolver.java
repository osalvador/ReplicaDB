package org.replicadb.server.job.execution;

import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class JobDefinitionEnvResolver {

    private static final Pattern ENV_REFERENCE = Pattern.compile(
            "\\$\\{env:([A-Za-z_][A-Za-z0-9_]*)}");

    private final Function<String, String> environmentLookup;

    public JobDefinitionEnvResolver() {
        this(System::getenv);
    }

    public JobDefinitionEnvResolver(Function<String, String> environmentLookup) {
        this.environmentLookup = Objects.requireNonNull(environmentLookup, "environmentLookup must not be null");
    }

    public String resolve(String template) {
        if (template == null) {
            return null;
        }
        if (template.contains("${secret:")) {
            throw new UnsupportedOperationException("Secret references are not yet supported");
        }

        Matcher matcher = ENV_REFERENCE.matcher(template);
        StringBuffer resolved = new StringBuffer();
        while (matcher.find()) {
            String variableName = matcher.group(1);
            String value = environmentLookup.apply(variableName);
            if (value == null) {
                throw new IllegalArgumentException("Missing environment variable: " + variableName);
            }
            matcher.appendReplacement(resolved, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(resolved);
        return resolved.toString();
    }
}