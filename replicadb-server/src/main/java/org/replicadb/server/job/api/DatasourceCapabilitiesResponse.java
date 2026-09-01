package org.replicadb.server.job.api;

import java.util.Set;

public record DatasourceCapabilitiesResponse(
        boolean sourceCapable,
        boolean sinkCapable,
        Set<String> sourceModes,
        Set<String> sinkModes,
        boolean sourceQuery,
        boolean singleJobOnly) {
}