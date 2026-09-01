package org.replicadb.server.job.execution;

import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.ClaimedRunPreparation;
import org.replicadb.server.job.domain.JobRun;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Profile("api")
public class RunExecutionCoordinator {

    private static final Logger LOG = LogManager.getLogger(RunExecutionCoordinator.class);

    private final RunLeaseService runLeaseService;
    private final JobExecutionService jobExecutionService;
    private final ActiveRunRegistry activeRunRegistry;
    private final ExecutorService executor;

    @Autowired
    public RunExecutionCoordinator(RunLeaseService runLeaseService,
                                   JobExecutionService jobExecutionService,
                                   ActiveRunRegistry activeRunRegistry,
                                   @Value("${replicadb.server.execution.pool-size:4}") int poolSize) {
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        this.runLeaseService = runLeaseService;
        this.jobExecutionService = jobExecutionService;
        this.activeRunRegistry = activeRunRegistry;
        this.executor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new RunThreadFactory());
    }

    public void submit(UUID runId, String executorIdentity) {
        executor.submit(() -> {
            try {
                Optional<ClaimedRunPreparation> claimed = runLeaseService.claimAndPrepare(
                        runId, executorIdentity, Duration.ofMinutes(5));
                claimed.ifPresent(preparation -> jobExecutionService.executeClaimedRun(preparation, handle -> { }));
            } catch (RuntimeException exception) {
                LOG.error("Managed execution failed for run {}", runId, exception);
            }
        });
    }

    public boolean requestCancellation(UUID runId) {
        return activeRunRegistry.requestCancellation(runId);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdown();
    }

    boolean isShutdown() {
        return executor.isShutdown();
    }

    private static final class RunThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "ReplicadbRun-" + sequence.incrementAndGet());
        }
    }
}
