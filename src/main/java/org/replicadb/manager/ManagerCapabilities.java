package org.replicadb.manager;

import org.replicadb.cli.ReplicationMode;

import java.util.Objects;
import java.util.Set;

public record ManagerCapabilities(
        Set<ReplicationMode> sourceModes,
        Set<ReplicationMode> sinkModes,
        boolean sourceQuery,
        boolean singleJobOnly) {

    private static final Set<ReplicationMode> ALL_MODES = Set.of(
            ReplicationMode.COMPLETE, ReplicationMode.COMPLETE_ATOMIC, ReplicationMode.INCREMENTAL);
    private static final Set<ReplicationMode> COMPLETE_ONLY = Set.of(ReplicationMode.COMPLETE);
    private static final Set<ReplicationMode> COMPLETE_INCREMENTAL = Set.of(
            ReplicationMode.COMPLETE, ReplicationMode.INCREMENTAL);

    public ManagerCapabilities {
        sourceModes = Set.copyOf(Objects.requireNonNull(sourceModes, "sourceModes must not be null"));
        sinkModes = Set.copyOf(Objects.requireNonNull(sinkModes, "sinkModes must not be null"));
    }

    public boolean supportsSource() {
        return !sourceModes.isEmpty();
    }

    public boolean supportsSink() {
        return !sinkModes.isEmpty();
    }

    public boolean supports(DataSourceType dataSourceType, ReplicationMode mode) {
        Objects.requireNonNull(dataSourceType, "dataSourceType must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        return (dataSourceType == DataSourceType.SOURCE ? sourceModes : sinkModes).contains(mode);
    }

    public boolean supportsIncremental(DataSourceType dataSourceType) {
        return supports(dataSourceType, ReplicationMode.INCREMENTAL);
    }

    public static ManagerCapabilities forManager(SupportedManagers manager) {
        Objects.requireNonNull(manager, "manager must not be null");
        return switch (manager) {
            case MYSQL, MARIADB, POSTGRES, ORACLE, SQLSERVER, DB2, DB2_AS400 ->
                    new ManagerCapabilities(ALL_MODES, ALL_MODES, true, false);
            case SQLITE -> new ManagerCapabilities(ALL_MODES, COMPLETE_INCREMENTAL, true, false);
            case DENODO -> new ManagerCapabilities(ALL_MODES, Set.of(), true, false);
            case FILE -> new ManagerCapabilities(ALL_MODES, COMPLETE_INCREMENTAL, false, true);
            case KAFKA, S3 -> new ManagerCapabilities(Set.of(), COMPLETE_ONLY, false, false);
            case MONGODB, MONGODBSRV ->
                    new ManagerCapabilities(ALL_MODES, COMPLETE_INCREMENTAL, true, false);
            case HSQLDB, CUBRID, JTDS_SQLSERVER, NETEZZA ->
                    new ManagerCapabilities(COMPLETE_ONLY, COMPLETE_ONLY, true, true);
        };
    }
}
