package org.replicadb.server.job.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.config.RunLogRetentionConfiguration;
import org.replicadb.server.job.persistence.RunLogRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("api")
public class RunLogRetentionTask {

    private static final Logger LOG = LogManager.getLogger(RunLogRetentionTask.class);

    private final RunLogRepository repository;
    private final RunLogRetentionConfiguration configuration;

    public RunLogRetentionTask(RunLogRepository repository, RunLogRetentionConfiguration configuration) {
        this.repository = repository;
        this.configuration = configuration;
    }

    @Scheduled(cron = "0 45 3 * * *")
    void purgeExpiredOnSchedule() {
        purgeExpired();
    }

    int purgeExpired() {
        int deleted = repository.deleteOlderThan(configuration.retentionDays(), configuration.batchSize());
        LOG.info("Purged {} expired run logs", deleted);
        return deleted;
    }
}
