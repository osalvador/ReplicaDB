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
        assertEquals(Duration.ofMillis(100), properties.getAdmission().getJitterMax());
        assertEquals(Duration.ofMillis(250), properties.getAdmission().getGenericCooldown());
        assertEquals(1_024, properties.getAdmission().getDirectedQueueCapacity());
        assertTrue(properties.getAdmission().getAdaptiveBackoff().isEnabled());
    }

    @Test
    void acceptsZeroJitterAndCooldownWhenBackoffIsValid() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        properties.getAdmission().setJitterMax(Duration.ZERO);
        properties.getAdmission().setGenericCooldown(Duration.ZERO);

        assertDoesNotThrow(() -> properties.validate(8));
    }

    @Test
    void rejectsNegativeOptionalAdmissionDuration() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        properties.getAdmission().setJitterMax(Duration.ofMillis(-1));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> properties.validate(8));

        assertTrue(exception.getMessage().contains("jitter-max"));
    }

    @Test
    void rejectsInvalidEnabledBackoff() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        properties.getAdmission().getAdaptiveBackoff().setInitialDelay(Duration.ZERO);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> properties.validate(8));

        assertTrue(exception.getMessage().contains("initial-delay"));
    }

    @Test
    void allowsZeroBackoffValuesOnlyWhenBackoffIsDisabled() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        WorkerRuntimeProperties.AdaptiveBackoff backoff = properties.getAdmission().getAdaptiveBackoff();
        backoff.setEnabled(false);
        backoff.setInitialDelay(Duration.ZERO);
        backoff.setMaxDelay(Duration.ZERO);
        backoff.setDecayHalfLife(Duration.ZERO);

        assertDoesNotThrow(() -> properties.validate(8));
    }

    @Test
    void rejectsBackoffMaximumBelowInitialDelay() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        WorkerRuntimeProperties.AdaptiveBackoff backoff = properties.getAdmission().getAdaptiveBackoff();
        backoff.setInitialDelay(Duration.ofSeconds(2));
        backoff.setMaxDelay(Duration.ofSeconds(1));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> properties.validate(8));

        assertTrue(exception.getMessage().contains("max-delay"));
    }

    @Test
    void rejectsDirectedQueueCapacityOutsideBound() {
        WorkerRuntimeProperties properties = new WorkerRuntimeProperties();
        properties.getAdmission().setDirectedQueueCapacity(100_001);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> properties.validate(8));

        assertTrue(exception.getMessage().contains("directed-queue-capacity"));
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
        order.verify(coordinator).cancelPendingAdmissionsAndActiveRuns(Duration.ofSeconds(2));
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