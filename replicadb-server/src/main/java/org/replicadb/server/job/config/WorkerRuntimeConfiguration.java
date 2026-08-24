package org.replicadb.server.job.config;

import org.replicadb.server.job.application.RunLeaseService;
import org.replicadb.server.job.application.RunDispatchService;
import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.ActiveRunRegistry;
import org.replicadb.server.job.execution.HeartbeatService;
import org.replicadb.server.job.execution.JobExecutionService;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;
import org.replicadb.server.job.execution.WorkerRunIdentity;
import org.replicadb.server.job.port.JobRunStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;
import java.util.concurrent.Executors;

@Configuration(proxyBeanMethods = false)
@Profile("worker")
@EnableConfigurationProperties(WorkerRuntimeProperties.class)
public class WorkerRuntimeConfiguration {

    @Bean
    public HeartbeatService workerHeartbeatService(RunLeaseService runLeaseService,
                                                   WorkerRuntimeProperties properties) {
        return new HeartbeatService(runLeaseService, properties.getHeartbeatInterval(),
                properties.getLeaseDuration(),
                Executors.newSingleThreadScheduledExecutor(), properties.getShutdownTimeout());
    }

    @Bean
    public WorkerDispatchCoordinator workerDispatchCoordinator(RunLeaseService runLeaseService,
                                                               JobRunStore jobRunStore,
                                                               JobExecutionService jobExecutionService,
                                                               ActiveRunRegistry activeRunRegistry,
                                                               HeartbeatService heartbeatService,
                                                               WorkerRuntimeProperties properties) {
        return new WorkerDispatchCoordinator(runLeaseService, jobRunStore, jobExecutionService,
                activeRunRegistry, heartbeatService,
                WorkerRunIdentity.resolve(properties.getIdentity()), properties.getMaxConcurrentRuns(),
                properties.getLeaseDuration(), properties.getShutdownTimeout());
    }

    @Bean
    public PollingFallback workerPollingFallback(WorkerDispatchCoordinator workerCoordinator,
                                                 JobRunStore jobRunStore,
                                                 RunDispatchService runDispatchService,
                                                 WorkerRuntimeProperties properties) {
        return new PollingFallback(workerCoordinator, jobRunStore, runDispatchService,
                workerCoordinator.workerIdentity().value(), properties.getPollInterval(),
                properties.getPollBatchSize(), PollingFallback.newScheduler(), properties.getShutdownTimeout());
    }

    @Bean
    public PostgreSQLNotificationListener workerNotificationListener(DataSource dataSource,
                                                                      WorkerDispatchCoordinator workerCoordinator,
                                                                      PollingFallback pollingFallback,
                                                                      WorkerRuntimeProperties properties) {
        return new PostgreSQLNotificationListener(dataSource, workerCoordinator, pollingFallback,
                properties.getListener().getInitialReconnectDelay(),
                properties.getListener().getMaxReconnectDelay(), properties.getShutdownTimeout());
    }

    @Bean
    public WorkerRuntimeLifecycle workerRuntimeLifecycle(WorkerDispatchCoordinator workerCoordinator,
                                                         PollingFallback pollingFallback,
                                                         PostgreSQLNotificationListener notificationListener,
                                                         HeartbeatService heartbeatService,
                                                         WorkerRuntimeProperties properties,
                                                         @Value("${spring.datasource.hikari.maximum-pool-size:8}")
                                                         int datasourcePoolSize) {
        properties.validate(datasourcePoolSize);
        return new WorkerRuntimeLifecycle(workerCoordinator, pollingFallback, notificationListener,
                heartbeatService, properties.getShutdownTimeout());
    }
}