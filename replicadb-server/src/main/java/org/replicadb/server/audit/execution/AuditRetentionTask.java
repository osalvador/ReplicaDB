package org.replicadb.server.audit.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.audit.persistence.AuditEventRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AuditRetentionTask {

    private static final Logger LOG = LogManager.getLogger(AuditRetentionTask.class);

    private final AuditEventRepository repository;
    private final int retentionDays;

    public AuditRetentionTask(AuditEventRepository repository,
                              @Value("${replicadb.server.audit.retention-days:365}") int retentionDays) {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("retentionDays must be positive");
        }
        this.repository = repository;
        this.retentionDays = retentionDays;
    }

    @Scheduled(cron = "0 30 3 * * *")
    void purgeExpiredOnSchedule() {
        purgeExpired();
    }

    int purgeExpired() {
        int deleted = repository.deleteOlderThan(retentionDays);
        LOG.info("Purged {} expired audit events", deleted);
        return deleted;
    }
}
