package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Schema(description = "Redacted managed datasource profile and permission-aware capabilities.")
public record DatasourceResponse(
        @Schema(description = "Datasource identifier.", format = "uuid") UUID id,
        @Schema(description = "Datasource display name.") String name,
        @Schema(description = "Registered connector wire name.") String connectorType,
        @Schema(description = "Credential-redacted connection display suitable for the UI.") String safeConnectDisplay,
        @Schema(description = "Non-secret connector settings.") Map<String, String> technicalParams,
        @Schema(description = "Whether encrypted security material is configured, without exposing it.") boolean securityConfigured,
        @Schema(description = "Role and mode capabilities for the connector.") DatasourceCapabilitiesResponse capabilities,
        @Schema(description = "Whether the current identity may view this profile.") boolean canView,
        @Schema(description = "Whether the current identity may bind this profile to a job.") boolean canUse,
        @Schema(description = "Whether the current identity may edit this profile.") boolean canEdit,
        @Schema(description = "Creation timestamp in UTC.", format = "date-time") Instant createdAt,
        @Schema(description = "Last update timestamp in UTC.", format = "date-time") Instant updatedAt) {
}
