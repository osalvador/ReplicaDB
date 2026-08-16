package org.replicadb.server.job.api;

import java.util.List;

public record PageResponse<T>(List<T> content, int page, int size, long totalElements) {

    public PageResponse {
        content = List.copyOf(content);
    }
}
