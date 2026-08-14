package org.replicadb.execution;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationCancelledExceptionTest {

    @Test
    void isAnSQLExceptionWithTheProvidedMessage() {
        SQLException exception = new ReplicationCancelledException("replication cancelled");

        assertTrue(exception instanceof ReplicationCancelledException);
        assertEquals("replication cancelled", exception.getMessage());
    }
}