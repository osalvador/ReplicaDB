package org.replicadb.server.job.domain;

public record AzureAuthentication(
        String mode,
        String principalId,
        String loginHint,
        String clientCertificate,
        String clientKey) {
}
