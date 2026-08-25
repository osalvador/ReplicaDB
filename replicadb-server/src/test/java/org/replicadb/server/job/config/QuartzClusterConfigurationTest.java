package org.replicadb.server.job.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.quartz.JobStoreType;
import org.springframework.boot.autoconfigure.quartz.QuartzProperties;
import org.replicadb.server.config.QuartzClusterConfiguration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuartzClusterConfigurationTest {

    @Test
    void acceptsJdbcClusteredConfiguration() {
        QuartzClusterConfiguration configuration = new QuartzClusterConfiguration(true,
                quartzProperties(true, true));

        assertDoesNotThrow(configuration::validateClusterConfiguration);
    }

    @Test
    void allowsNonClusteredConfigurationWhenGuardIsDisabled() {
        QuartzClusterConfiguration configuration = new QuartzClusterConfiguration(false,
                quartzProperties(false, false));

        assertDoesNotThrow(configuration::validateClusterConfiguration);
    }

    @Test
    void rejectsRamJobStoreWhenClusteredRuntimeIsRequired() {
        QuartzClusterConfiguration configuration = new QuartzClusterConfiguration(true,
            quartzProperties(false, false));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            configuration::validateClusterConfiguration);

        assertTrue(exception.getMessage().contains("Clustered Quartz is required"));
    }

    @Test
    void rejectsJdbcJobStoreWithoutClustering() {
        QuartzClusterConfiguration configuration = new QuartzClusterConfiguration(true,
            quartzProperties(true, false));

        assertThrows(IllegalStateException.class, configuration::validateClusterConfiguration);
    }

    private static QuartzProperties quartzProperties(boolean jdbc, boolean clustered) {
        QuartzProperties properties = new QuartzProperties();
        properties.setJobStoreType(jdbc ? JobStoreType.JDBC : JobStoreType.MEMORY);
        properties.getProperties().put("org.quartz.jobStore.isClustered", Boolean.toString(clustered));
        return properties;
    }
}
