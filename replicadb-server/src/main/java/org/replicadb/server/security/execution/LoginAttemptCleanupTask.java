package org.replicadb.server.security.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.security.persistence.LoginAttemptRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("api")
public class LoginAttemptCleanupTask {

    private static final Logger LOG = LogManager.getLogger(LoginAttemptCleanupTask.class);

    private final LoginAttemptRepository repository;

    public LoginAttemptCleanupTask(LoginAttemptRepository repository) {
        this.repository = repository;
    }

    @Scheduled(cron = "0 */15 * * * *")
    void purgeExpiredOnSchedule() {
        purgeExpired();
    }

    int purgeExpired() {
        int deleted = repository.deleteExpired();
        LOG.info("Purged {} expired login-attempt rows", deleted);
        return deleted;
    }
}
