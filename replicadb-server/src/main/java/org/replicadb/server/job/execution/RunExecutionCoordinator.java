package org.replicadb.server.job.execution;

import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RunExecutionCoordinator {

    private static final Logger LOG = LogManager.getLogger(RunExecutionCoordinator.class);

    private final RunLeaseService runLeaseService;
    private final JobExecutionService jobExecutionService;
    private final ExecutorService executor;
    private final ConcurrentMap<UUID, ToolOptions> inFlight = new ConcurrentHashMap<>();

    @Autowired
    public RunExecutionCoordinator(RunLeaseService runLeaseService,
                                   JobExecutionService jobExecutionService,
                                   @Value("${replicadb.server.execution.pool-size:4}") int poolSize) {
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        this.runLeaseService = runLeaseService;
        this.jobExecutionService = jobExecutionService;
        this.executor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new RunThreadFactory());
    }

    public RunExecutionCoordinator(JobRunRepository jobRunRepository,
                                   JobExecutionService jobExecutionService, int poolSize) {
        this(new RunLeaseService(jobRunRepository), jobExecutionService, poolSize);
    }

    public void submit(UUID runId, String executorIdentity) {
        executor.submit(() -> {
            try {
                Optional<JobRun> claimed = runLeaseService.claimRequested(
                        runId, executorIdentity, Duration.ofMinutes(5));
                claimed.ifPresent(run -> jobExecutionService.executeClaimedRun(run,
                        options -> inFlight.put(runId, options)));
            } catch (RuntimeException exception) {
                LOG.error("Managed execution failed for run {}", runId, exception);
            } finally {
                inFlight.remove(runId);
            }
        });
    }

    public boolean requestCancellation(UUID runId) {
        ToolOptions options = inFlight.get(runId);
        if (options == null) {
            return false;
        }
        options.getExecutionContext().requestCancellation();
        return true;
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
