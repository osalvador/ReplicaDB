package org.replicadb.server.security.auth;

public class TooManyAttemptsException extends RuntimeException {

    public TooManyAttemptsException() {
        super("Too many failed login attempts");
    }
}
