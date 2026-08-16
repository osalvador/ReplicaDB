package org.replicadb.server.job.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record JobDefinitionRequest(
        @NotBlank(groups = Create.class)
        String name,
        @NotBlank String sourceConnect,
        String sourceUser,
        String sourcePassword,
        @NotBlank String sourceTable,
        String sourceWhere,
        @NotBlank String sinkConnect,
        String sinkUser,
        String sinkPassword,
        @NotBlank String sinkTable,
        @NotBlank String mode,
        @Min(1) int jobs,
        String incrementalWatermarkColumn,
        String initialWatermarkValue) {

    public interface Create {
    }
}
