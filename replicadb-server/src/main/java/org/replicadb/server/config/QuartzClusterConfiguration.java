package org.replicadb.server.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.quartz.JobStoreType;
import org.springframework.boot.autoconfigure.quartz.QuartzProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import jakarta.annotation.PostConstruct;

import java.util.Locale;

@Configuration(proxyBeanMethods = false)
@Profile("api")
public class QuartzClusterConfiguration {

    public QuartzClusterConfiguration(
            @Value("${replicadb.server.scheduler.clustered-required:true}") boolean clusteredRequired,
            QuartzProperties quartzProperties) {
        this.clusteredRequired = clusteredRequired;
        this.quartzProperties = quartzProperties;
        }

        private final boolean clusteredRequired;
        private final QuartzProperties quartzProperties;

        @PostConstruct
        public void validateClusterConfiguration() {
        boolean clustered = quartzProperties.getProperties().entrySet().stream()
            .filter(entry -> normalize(entry.getKey()).equals("org.quartz.jobstore.isclustered"))
            .map(entry -> entry.getValue())
            .findFirst()
            .map(Boolean::parseBoolean)
            .orElse(false);
        if (clusteredRequired && (quartzProperties.getJobStoreType() != JobStoreType.JDBC || !clustered)) {
            throw new IllegalStateException(
                    "Clustered Quartz is required; configure spring.quartz.job-store-type=jdbc "
                        + "and org.quartz.jobStore.isClustered=true");
        }
    }

    private static String normalize(String property) {
        return property == null ? "" : property.toLowerCase(Locale.ROOT).replace("-", "");
    }
}
