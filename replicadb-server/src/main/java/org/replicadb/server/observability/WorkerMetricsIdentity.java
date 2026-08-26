package org.replicadb.server.observability;

import java.util.regex.Pattern;

public final class WorkerMetricsIdentity {

    public static final int MAX_LENGTH = 64;
    private static final Pattern UNSAFE_CHARACTERS = Pattern.compile("[^A-Za-z0-9._-]");

    private WorkerMetricsIdentity() {
    }

    public static String normalize(String identity) {
        if (identity == null || identity.isBlank()) {
            return "other";
        }
        String normalized = UNSAFE_CHARACTERS.matcher(identity.trim()).replaceAll("_");
        if (normalized.isBlank()) {
            return "other";
        }
        return normalized.length() > MAX_LENGTH ? normalized.substring(0, MAX_LENGTH) : normalized;
    }
}