package org.replicadb.server.job.execution;

import org.replicadb.server.job.persistence.RunTriggerIdempotencyRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class IdempotencyCleanupTask {

    private final RunTriggerIdempotencyRepository repository;

    public IdempotencyCleanupTask(RunTriggerIdempotencyRepository repository) {
        this.repository = repository;
    }

    @Scheduled(cron = "0 0 3 * * *")
    void purgeExpiredOnSchedule() {
        purgeExpired();
    }

    int purgeExpired() {
        return repository.deleteExpired();
    }
}
