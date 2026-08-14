package org.replicadb.execution;

import java.sql.SQLException;

public final class ReplicationCancelledException extends SQLException {

    public ReplicationCancelledException(String message) {
        super(message);
    }

    public ReplicationCancelledException(String message, Throwable cause) {
        super(message, cause);
    }
}