package org.replicadb.server;

import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.replicadb.server.job.domain.JobDefinitionTestFixtures;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.job.api.JobRunController;
import org.replicadb.server.job.config.WorkerRuntimeLifecycle;
import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.HeartbeatService;
import org.replicadb.server.job.execution.RunExecutionCoordinator;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobRunRepository;
import org.replicadb.server.job.persistence.PostgresNotificationPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.session.SessionRepository;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.context.WebApplicationContext;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("worker")
@Import(PostgresTestcontainersConfig.class)
class WorkerProfileContextTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private JobDefinitionRepository jobDefinitionRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private PostgresNotificationPublisher notificationPublisher;

    @Autowired
    private ServerProperties serverProperties;

    @Test
    void loadsSharedPostgresStateWithoutApiSurface() throws Exception {
        assertTrue(applicationContext.getBeansOfType(DataSource.class).size() >= 1);
        assertTrue(applicationContext.getBeansOfType(JobRunRepository.class).size() >= 1);
        assertTrue(applicationContext.getBeansOfType(WorkerDispatchCoordinator.class).size() == 1);
        assertTrue(applicationContext.getBeansOfType(PollingFallback.class).size() == 1);
        assertTrue(applicationContext.getBeansOfType(PostgreSQLNotificationListener.class).size() == 1);
        assertTrue(applicationContext.getBeansOfType(HeartbeatService.class).size() == 1);
        assertTrue(applicationContext.getBeansOfType(WorkerRuntimeLifecycle.class).size() == 1);
        assertTrue(applicationContext.getBean(WorkerRuntimeLifecycle.class).isRunning());
        assertTrue(applicationContext.getBean(PollingFallback.class).isRunning());
        assertTrue(applicationContext.getBean(PostgreSQLNotificationListener.class).isRunning());
        assertTrue(applicationContext instanceof WebApplicationContext);
        assertEquals(-1, serverProperties.getPort());
        assertTrue(applicationContext.getBeansOfType(SecurityFilterChain.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(SessionRepository.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(Scheduler.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(JobRunController.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(RunExecutionCoordinator.class).isEmpty());
        assertFalse(applicationContext.containsBean("securityConfig"));
        assertFalse(applicationContext.containsBean("scheduleReconciler"));
        assertFalse(applicationContext.containsBean("adminBootstrapRunner"));

        PollingFallback polling = applicationContext.getBean(PollingFallback.class);
        polling.stop();
        JobRun pending = jobRunRepository.insertPendingNow(
                jobDefinitionRepository.insert(JobDefinitionTestFixtures.aJobDefinition()
                        .withName("worker-context-" + java.util.UUID.randomUUID())
                        .build()).id(), null, 1);
        notificationPublisher.publishRun(pending.id());

        await(() -> jobRunRepository.findById(pending.id())
                .map(run -> run.status() != JobRunStatus.PENDING)
                .orElse(false));
        JobRun claimed = jobRunRepository.findById(pending.id()).orElseThrow();
        assertNotNull(claimed.executorIdentity());
        assertNotEquals(JobRunStatus.PENDING, claimed.status());

        WorkerRuntimeLifecycle lifecycle = applicationContext.getBean(WorkerRuntimeLifecycle.class);
        lifecycle.stop();
        assertFalse(lifecycle.isRunning());
        assertFalse(polling.isRunning());
        assertFalse(applicationContext.getBean(PostgreSQLNotificationListener.class).isRunning());
        assertTrue(applicationContext.getBean(PostgreSQLNotificationListener.class).isShutdown());
        assertTrue(applicationContext.getBean(WorkerDispatchCoordinator.class).isShutdown());
    }

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && !check.completed()) {
            Thread.sleep(10);
        }
        assertTrue(check.completed());
    }

    @FunctionalInterface
    private interface Check {
        boolean completed();
    }
}
