package org.replicadb.server.job.execution;

import org.replicadb.cli.AzureAuthenticationOptions;
import org.replicadb.cli.ToolOptions;
import org.replicadb.cli.ToolOptionsBuilder;
import org.replicadb.server.job.domain.AzureAuthentication;
import org.replicadb.server.job.domain.ResolvedDataSource;
import org.replicadb.server.job.domain.JobDefinition;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@Component
@Profile({"api", "worker"})
public final class ManagedToolOptionsFactory {

    public ToolOptions build(ResolvedJobDefinition resolvedJob, String previousWatermarkValue) {
        Objects.requireNonNull(resolvedJob, "resolvedJob must not be null");
        JobDefinition definition = resolvedJob.definition();
        ResolvedDataSource source = resolvedJob.sourceDataSource();
        ResolvedDataSource sink = resolvedJob.sinkDataSource();
        ToolOptionsBuilder builder = new ToolOptionsBuilder()
                .sourceConnect(source.connect())
                .sourceUser(source.user())
                .sourcePassword(source.password())
                .sourceAuthentication(authentication(source.authentication()))
                .sourceTable(definition.sourceTable())
                .sourceColumns(definition.sourceColumns())
                .sourceWhere(definition.sourceWhere())
                .sourceQuery(definition.sourceQuery())
                .sourceConnectionParams(connectionParameters(source))
                .sinkConnect(sink.connect())
                .sinkUser(sink.user())
                .sinkPassword(sink.password())
                .sinkAuthentication(authentication(sink.authentication()))
                .sinkTable(definition.sinkTable())
                .sinkColumns(definition.sinkColumns())
                .sinkStagingSchema(definition.sinkStagingSchema())
                .sinkStagingTable(definition.sinkStagingTable())
                .sinkConnectionParams(connectionParameters(sink))
                .sinkDisableEscape(definition.sinkDisableEscape())
                .sinkDisableTruncate(definition.sinkDisableTruncate())
                .mode(definition.mode().getModeText())
                .jobs(definition.jobs())
                .fetchSize(definition.fetchSize())
                .bandwidthThrottling(definition.bandwidthThrottling())
                .verbose(definition.verbose())
                .incrementalWatermarkColumn(definition.incrementalWatermarkColumn());
        if (definition.incrementalWatermarkColumn() != null) {
            builder.incrementalWatermarkValue(previousWatermarkValue == null
                    ? definition.initialWatermarkValue() : previousWatermarkValue);
        }
        return builder.build();
    }

    private static AzureAuthenticationOptions authentication(AzureAuthentication authentication) {
        AzureAuthenticationOptions options = new AzureAuthenticationOptions();
        if (authentication == null) {
            return options;
        }
        options.setMode(authentication.mode());
        options.setPrincipalId(authentication.principalId());
        options.setLoginHint(authentication.loginHint());
        options.setClientCertificate(authentication.clientCertificate());
        options.setClientKey(authentication.clientKey());
        return options;
    }

    private static Properties connectionParameters(ResolvedDataSource dataSource) {
        Properties properties = new Properties();
        dataSource.technicalParams().forEach(properties::setProperty);
        for (Map.Entry<String, String> entry : dataSource.securityParams().entrySet()) {
            if (entry.getKey().startsWith("connect.parameter.")) {
                properties.setProperty(entry.getKey().substring("connect.parameter.".length()), entry.getValue());
            }
        }
        return properties;
    }
}
