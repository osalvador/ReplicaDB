package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AzureAuthenticationTest {

    @Test
    void storesAuthenticationFields() {
        AzureAuthentication authentication = new AzureAuthentication(
                "ActiveDirectoryDefault", "client-id", "login@example.test", "certificate.pem", "client.key");

        assertEquals("ActiveDirectoryDefault", authentication.mode());
        assertEquals("client-id", authentication.principalId());
        assertEquals("login@example.test", authentication.loginHint());
        assertEquals("certificate.pem", authentication.clientCertificate());
        assertEquals("client.key", authentication.clientKey());
    }
}
