package org.replicadb.server.job.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RunLogRetentionConfiguration {

    private final int retentionDays;
    private final int batchSize;

    public RunLogRetentionConfiguration(
            @Value("${replicadb.server.run-log.retention-days:${replicadb.server.audit.retention-days:365}}") int retentionDays,
            @Value("${replicadb.server.run-log.batch-size:1000}") int batchSize) {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("retentionDays must be positive");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.retentionDays = retentionDays;
        this.batchSize = batchSize;
    }

    public int retentionDays() {
        return retentionDays;
    }

    public int batchSize() {
        return batchSize;
    }
}
