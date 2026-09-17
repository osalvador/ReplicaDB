package org.replicadb.server.security.api;

public record KeyringReencryptResponse(int reencrypted, long remaining, boolean converged) {
}
