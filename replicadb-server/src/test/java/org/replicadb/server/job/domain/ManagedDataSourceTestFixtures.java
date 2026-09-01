package org.replicadb.server.job.domain;

import java.util.Map;
import java.util.UUID;

public final class ManagedDataSourceTestFixtures {

    public static final UUID SOURCE_DATASOURCE_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    public static final UUID SINK_DATASOURCE_ID = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private ManagedDataSourceTestFixtures() {
    }

    public static ManagedDataSource source() {
        return dataSource(SOURCE_DATASOURCE_ID, "test-source-datasource");
    }

    public static ManagedDataSource sink() {
        return dataSource(SINK_DATASOURCE_ID, "test-sink-datasource");
    }

    private static ManagedDataSource dataSource(UUID id, String name) {
        return new ManagedDataSource(id, name, ConnectorType.POSTGRES,
                "jdbc:postgresql://[REDACTED]/replicadb", Map.of("sslmode", "require"),
                new byte[]{1, 2, 3}, 1, "AES-256-GCM", "test", null, null);
    }
}
