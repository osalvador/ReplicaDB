package org.replicadb.server.job.application;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public final class RunCancellationService {

    private static final Logger LOG = LogManager.getLogger(RunCancellationService.class);

    private final JobRunStore runStore;
    private final RunNotificationPublisher notificationPublisher;

    @Autowired
    public RunCancellationService(JobRunStore runStore, RunNotificationPublisher notificationPublisher) {
        this.runStore = Objects.requireNonNull(runStore, "runStore must not be null");
        this.notificationPublisher = Objects.requireNonNull(notificationPublisher,
                "notificationPublisher must not be null");
    }

    @Deprecated
    public RunCancellationService(JobRunStore runStore) {
        this(runStore, new RunNotificationPublisher() {
            @Override
            public void publishRun(UUID runId) {
            }

            @Override
            public void publishCancellation(UUID runId) {
            }
        });
    }

    public JobRunStore.CancellationResult requestCancellation(UUID runId, String cancellationWarning,
                                                               Consumer<UUID> localSignal) {
        Objects.requireNonNull(runId, "runId must not be null");
        Objects.requireNonNull(localSignal, "localSignal must not be null");
        JobRunStore.CancellationResult result = runStore.requestCancellation(runId, cancellationWarning);
        if (result == JobRunStore.CancellationResult.REQUESTED
                || result == JobRunStore.CancellationResult.ALREADY_REQUESTED) {
            try {
                notificationPublisher.publishCancellation(runId);
            } catch (RuntimeException exception) {
                LOG.warn("Cancellation notification failed for run {} with {}",
                        runId, exception.getClass().getSimpleName());
            }
            try {
                localSignal.accept(runId);
            } catch (RuntimeException exception) {
                LOG.warn("Local cancellation signal failed for run {} with {}",
                        runId, exception.getClass().getSimpleName());
            }
        }
        return result;
    }

    public JobRunStore.CancellationResult cancelPending(UUID runId, String cancellationWarning) {
        Objects.requireNonNull(runId, "runId must not be null");
        return runStore.cancelPending(runId, cancellationWarning);
    }
}
