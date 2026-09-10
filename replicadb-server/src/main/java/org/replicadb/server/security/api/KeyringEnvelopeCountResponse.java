package org.replicadb.server.security.api;

public record KeyringEnvelopeCountResponse(String keyVersion, long count, boolean known) {
}
