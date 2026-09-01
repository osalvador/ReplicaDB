package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagerCapabilitiesTest {

    @Test
    void everyRegisteredManagerHasImmutableCapabilities() {
        for (SupportedManagers manager : SupportedManagers.values()) {
            ManagerCapabilities capabilities = manager.getCapabilities();

            assertNotNull(capabilities);
            assertNotNull(capabilities.sourceModes());
            assertNotNull(capabilities.sinkModes());
            assertTrue(capabilities.sourceModes().containsAll(capabilities.sourceModes()));
            assertTrue(capabilities.sinkModes().containsAll(capabilities.sinkModes()));
        }
    }

    @Test
    void specializedRoleAndModeLimitsAreExplicit() {
        assertFalse(SupportedManagers.DENODO.getCapabilities().supportsSink());
        assertTrue(SupportedManagers.DENODO.getCapabilities().supportsSource());

        assertFalse(SupportedManagers.KAFKA.getCapabilities().supportsSource());
        assertTrue(SupportedManagers.KAFKA.getCapabilities().supports(DataSourceType.SINK,
                ReplicationMode.COMPLETE));
        assertFalse(SupportedManagers.KAFKA.getCapabilities().supports(DataSourceType.SINK,
                ReplicationMode.INCREMENTAL));

        assertFalse(SupportedManagers.S3.getCapabilities().supportsSource());
        assertFalse(SupportedManagers.S3.getCapabilities().supports(DataSourceType.SINK,
                ReplicationMode.COMPLETE_ATOMIC));
        assertFalse(SupportedManagers.MONGODB.getCapabilities().supports(DataSourceType.SINK,
                ReplicationMode.COMPLETE_ATOMIC));
        assertFalse(SupportedManagers.SQLITE.getCapabilities().supports(DataSourceType.SINK,
                ReplicationMode.COMPLETE_ATOMIC));
        assertFalse(SupportedManagers.FILE.getCapabilities().supports(DataSourceType.SINK,
                ReplicationMode.COMPLETE_ATOMIC));
    }

    @Test
    void genericManagersExposeStandardJdbcRestrictions() {
        for (SupportedManagers manager : new SupportedManagers[]{
                SupportedManagers.HSQLDB, SupportedManagers.CUBRID,
                SupportedManagers.JTDS_SQLSERVER, SupportedManagers.NETEZZA}) {
            ManagerCapabilities capabilities = manager.getCapabilities();

            assertTrue(capabilities.supports(DataSourceType.SOURCE, ReplicationMode.COMPLETE));
            assertTrue(capabilities.supports(DataSourceType.SINK, ReplicationMode.COMPLETE));
            assertFalse(capabilities.supports(DataSourceType.SOURCE, ReplicationMode.INCREMENTAL));
            assertFalse(capabilities.supports(DataSourceType.SINK, ReplicationMode.COMPLETE_ATOMIC));
            assertTrue(capabilities.singleJobOnly());
        }
    }
}
