package org.replicadb.server.job.config;

import org.junit.jupiter.api.Test;
import org.replicadb.server.job.dispatch.PollingFallback;
import org.replicadb.server.job.dispatch.PostgreSQLNotificationListener;
import org.replicadb.server.job.execution.HeartbeatService;
import org.replicadb.server.job.execution.WorkerDispatchCoordinator;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WorkerRuntimeConfigurationTest {

    @Test
    void acceptsTheDefaultPoolHeadroom() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();

        assertDoesNotThrow(() -> properties.validate(8));
    }

    @Test
    void rejectsDatasourcePoolSmallerThanConcurrencyHeadroom() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        properties.setMaxConcurrentRuns(5);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> properties.validate(8));

        assertTrue(exception.getMessage().contains("maximum-pool-size"));
        assertTrue(exception.getMessage().contains("9"));
    }

    @Test
    void startsIntakePollingAndListenerInOrder() {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PostgreSQLNotificationListener listener = mock(PostgreSQLNotificationListener.class);
        HeartbeatService heartbeat = mock(HeartbeatService.class);
        WorkerRuntimeLifecycle lifecycle = lifecycle(coordinator, polling, listener, heartbeat);

        lifecycle.start();

        var order = inOrder(coordinator, polling, listener);
        order.verify(coordinator).startAccepting();
        order.verify(polling).start();
        order.verify(listener).start();
        assertTrue(lifecycle.isRunning());
    }

    @Test
    void listenerStartupFailureDoesNotDisablePolling() {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PostgreSQLNotificationListener listener = mock(PostgreSQLNotificationListener.class);
        HeartbeatService heartbeat = mock(HeartbeatService.class);
        doThrow(new IllegalStateException("listener unavailable")).when(listener).start();
        WorkerRuntimeLifecycle lifecycle = lifecycle(coordinator, polling, listener, heartbeat);

        lifecycle.start();

        verify(coordinator).startAccepting();
        verify(polling).start();
        verify(listener).start();
        assertTrue(lifecycle.isRunning());
    }

    @Test
    void stopsIntakeBeforePollingListenerHeartbeatAndExecutor() {
        WorkerDispatchCoordinator coordinator = mock(WorkerDispatchCoordinator.class);
        PollingFallback polling = mock(PollingFallback.class);
        PostgreSQLNotificationListener listener = mock(PostgreSQLNotificationListener.class);
        HeartbeatService heartbeat = mock(HeartbeatService.class);
        WorkerRuntimeLifecycle lifecycle = lifecycle(coordinator, polling, listener, heartbeat);
        lifecycle.start();

        lifecycle.stop();

        var order = inOrder(coordinator, polling, listener, heartbeat);
        order.verify(coordinator).stopAccepting();
        order.verify(polling).stop();
        order.verify(listener).stop();
        order.verify(heartbeat).shutdown();
        order.verify(coordinator).shutdown(Duration.ofSeconds(2));
        assertEquals(false, lifecycle.isRunning());
    }

    private static WorkerRuntimeLifecycle lifecycle(WorkerDispatchCoordinator coordinator,
                                                    PollingFallback polling,
                                                    PostgreSQLNotificationListener listener,
                                                    HeartbeatService heartbeat) {
        return new WorkerRuntimeLifecycle(coordinator, polling, listener, heartbeat, Duration.ofSeconds(2));
    }
}