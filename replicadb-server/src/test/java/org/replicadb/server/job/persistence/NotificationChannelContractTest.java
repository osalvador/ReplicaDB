package org.replicadb.server.job.persistence;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.replicadb.server.job.port.RunNotificationPublisher;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationChannelContractTest {

    @Test
    void exposesOnlyTheFixedNotificationChannels() {
        assertEquals("replicadb_runs", RunNotificationPublisher.RUN_CHANNEL);
        assertEquals("replicadb_run_control", RunNotificationPublisher.CONTROL_CHANNEL);
    }

    @Test
    void publishesOnlyTheUuidPayloadOnTheSelectedChannel() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PostgresNotificationPublisher publisher = new PostgresNotificationPublisher(jdbcTemplate);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT pg_notify(?, ?)")).thenReturn(statement);
        ArgumentCaptor<ConnectionCallback<?>> callback = ArgumentCaptor.forClass(ConnectionCallback.class);
        UUID runId = UUID.randomUUID();

        publisher.publishRun(runId);
        publisher.publishCancellation(runId);

        verify(jdbcTemplate, org.mockito.Mockito.times(2)).execute(callback.capture());
        callback.getAllValues().get(0).doInConnection(connection);
        callback.getAllValues().get(1).doInConnection(connection);
        org.mockito.InOrder calls = inOrder(statement);
        calls.verify(statement).setString(1, RunNotificationPublisher.RUN_CHANNEL);
        calls.verify(statement).setString(2, runId.toString());
        calls.verify(statement).execute();
        calls.verify(statement).setString(1, RunNotificationPublisher.CONTROL_CHANNEL);
        calls.verify(statement).setString(2, runId.toString());
        calls.verify(statement).execute();
    }

    @Test
    void rejectsMissingNotificationIdentifiers() {
        PostgresNotificationPublisher publisher = new PostgresNotificationPublisher(mock(JdbcTemplate.class));

        assertThrows(NullPointerException.class, () -> publisher.publishRun(null));
        assertThrows(NullPointerException.class, () -> publisher.publishCancellation(null));
    }
}