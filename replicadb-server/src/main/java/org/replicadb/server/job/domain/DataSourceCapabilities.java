package org.replicadb.server.job.domain;

import org.replicadb.cli.ReplicationMode;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerCapabilities;

import java.util.Set;

public record DataSourceCapabilities(
        Set<ReplicationMode> sourceModes,
        Set<ReplicationMode> sinkModes,
        boolean sourceQuery,
        boolean singleJobOnly) {

    public static DataSourceCapabilities from(ManagerCapabilities capabilities) {
        return new DataSourceCapabilities(capabilities.sourceModes(), capabilities.sinkModes(),
                capabilities.sourceQuery(), capabilities.singleJobOnly());
    }

    public boolean supports(DataSourceType dataSourceType, ReplicationMode mode) {
        return (dataSourceType == DataSourceType.SOURCE ? sourceModes : sinkModes).contains(mode);
    }

    public boolean supportsSource() {
        return !sourceModes.isEmpty();
    }

    public boolean supportsSink() {
        return !sinkModes.isEmpty();
    }
}
