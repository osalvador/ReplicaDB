package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Set;

@Schema(description = "Role-specific connector capabilities used to validate job bindings.")
public record DatasourceCapabilitiesResponse(
        @Schema(description = "Whether the connector can provide source rows.") boolean sourceCapable,
        @Schema(description = "Whether the connector can receive sink rows.") boolean sinkCapable,
        @ArraySchema(schema = @Schema(description = "Supported source mode.", allowableValues = {"complete", "complete-atomic", "incremental"}))
        Set<String> sourceModes,
        @ArraySchema(schema = @Schema(description = "Supported sink mode.", allowableValues = {"complete", "complete-atomic", "incremental"}))
        Set<String> sinkModes,
        @Schema(description = "Whether source.query is supported.") boolean sourceQuery,
        @Schema(description = "Whether the connector requires jobs=1.") boolean singleJobOnly) {
}
