package org.replicadb.server.job.api;

public record PageRequestParams(int page, int size) {

    public static PageRequestParams of(Integer page, Integer size) {
        int resolvedPage = page == null ? 0 : page;
        if (resolvedPage < 0) {
            throw new IllegalArgumentException("page must not be negative");
        }
        int resolvedSize = size == null ? 50 : Math.max(1, Math.min(200, size));
        return new PageRequestParams(resolvedPage, resolvedSize);
    }
}
