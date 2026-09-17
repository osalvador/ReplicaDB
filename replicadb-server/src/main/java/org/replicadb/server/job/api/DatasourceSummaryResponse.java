package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.UUID;

@Schema(description = "Redacted datasource identity embedded in a job response.")
public record DatasourceSummaryResponse(
        @Schema(description = "Datasource identifier.", format = "uuid") UUID id,
        @Schema(description = "Datasource display name.") String name,
        @Schema(description = "Registered connector wire name.") String connectorType,
        @Schema(description = "Credential-redacted connection display.") String safeConnectDisplay) {
}
