package org.replicadb.server.job.api;

import org.replicadb.config.CredentialRedactor;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.server.job.domain.JobDefinition;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Component
public class JobDefinitionMapper {

    private static final String COMPLETE_MODE_WARNING =
            "An interrupted or retried complete run leaves the sink truncated or partially loaded.";

    public JobDefinition toDefinition(JobDefinitionRequest request, UUID id, String existingName,
                                      Instant createdAt, Instant updatedAt) {
        String name = request.name() == null ? existingName : request.name();
        return new JobDefinition(
                id, name, request.sourceConnect(), request.sourceUser(), request.sourcePassword(),
                request.sourceTable(), request.sourceWhere(), request.sinkConnect(), request.sinkUser(),
                request.sinkPassword(), request.sinkTable(), parseMode(request.mode()), request.jobs(),
                request.incrementalWatermarkColumn(), request.initialWatermarkValue(), createdAt, updatedAt);
    }

    public JobDefinitionResponse toResponse(JobDefinition definition) {
        String modeWarning = definition.mode() == ReplicationMode.COMPLETE ? COMPLETE_MODE_WARNING : null;
        return new JobDefinitionResponse(
            definition.id(), definition.name(), CredentialRedactor.redactConnectionString(definition.sourceConnect()),
            definition.sourceUser(),
                definition.sourceTable(), definition.sourceWhere(),
                CredentialRedactor.redactConnectionString(definition.sinkConnect()), definition.sinkUser(),
                definition.sinkTable(), definition.mode().getModeText(), definition.jobs(), definition.incrementalWatermarkColumn(),
                definition.initialWatermarkValue(), definition.createdAt(), definition.updatedAt(),
                definition.sourcePassword() != null, definition.sinkPassword() != null, modeWarning);
    }

    public static String completeModeWarning() {
        return COMPLETE_MODE_WARNING;
    }

    private static ReplicationMode parseMode(String modeText) {
        String normalized = modeText.toLowerCase(Locale.ROOT);
        for (ReplicationMode mode : ReplicationMode.values()) {
            if (mode.getModeText().equals(normalized) || mode.name().equalsIgnoreCase(modeText)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown replication mode: " + modeText);
    }
}
