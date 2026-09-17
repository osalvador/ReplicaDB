package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

@Schema(description = "Zero-based page of resource results.")
public record PageResponse<T>(
    @ArraySchema(arraySchema = @Schema(description = "Results in this page.")) List<T> content,
    @Schema(description = "Zero-based page number.", minimum = "0") int page,
    @Schema(description = "Effective page size, from 1 through 200.", minimum = "1", maximum = "200") int size,
    @Schema(description = "Total matching resources across all pages.", minimum = "0") long totalElements) {

    public PageResponse {
        content = List.copyOf(content);
    }
}
