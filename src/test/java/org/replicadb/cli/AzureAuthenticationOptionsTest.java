package org.replicadb.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AzureAuthenticationOptionsTest {

    @Test
    void parsesSupportedModesCaseInsensitively() {
        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE,
                AzureAuthenticationMode.fromValue("activedirectoryinteractive"));
        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_MANAGED_IDENTITY,
                AzureAuthenticationMode.fromValue("ActiveDirectoryMSI"));
        assertNull(AzureAuthenticationMode.fromValue("  "));
    }

    @Test
    void rejectsDeprecatedPasswordAuthentication() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> AzureAuthenticationMode.fromValue("ActiveDirectoryPassword"));

        assertTrue(exception.getMessage().contains("ActiveDirectoryInteractive"));
        assertTrue(exception.getMessage().contains("deprecated"));
    }

    @Test
    void validatesModeSpecificFields() {
        AzureAuthenticationOptions interactive = new AzureAuthenticationOptions();
        interactive.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE);
        interactive.setLoginHint("user@example.com");
        interactive.validate(false);

        AzureAuthenticationOptions servicePrincipal = new AzureAuthenticationOptions();
        servicePrincipal.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_SERVICE_PRINCIPAL);
        servicePrincipal.setPrincipalId("client-id");
        assertThrows(IllegalArgumentException.class, servicePrincipal::validate);
        servicePrincipal.validate(true);

        AzureAuthenticationOptions certificate = new AzureAuthenticationOptions();
        certificate.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE);
        certificate.setPrincipalId("client-id");
        certificate.setClientCertificate("/tmp/client.pfx");
        certificate.validate(false);

        AzureAuthenticationOptions managedIdentity = new AzureAuthenticationOptions();
        managedIdentity.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_MANAGED_IDENTITY);
        managedIdentity.validate(false);
    }

    @Test
    void rejectsFieldsThatConflictWithTheSelectedMode() {
        AzureAuthenticationOptions interactive = new AzureAuthenticationOptions();
        interactive.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE);
        interactive.setPrincipalId("client-id");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> interactive.validate(false));

        assertTrue(exception.getMessage().contains("principal ID"));
    }

    @Test
    void sourceAndSinkOptionsRemainIndependentAndContainNoSecrets() {
        AzureAuthenticationOptions source = new AzureAuthenticationOptions();
        source.setMode("ActiveDirectoryInteractive");
        source.setLoginHint("user@example.com");

        AzureAuthenticationOptions sink = new AzureAuthenticationOptions();
        sink.setMode("ActiveDirectoryDefault");

        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE, source.getMode());
        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT, sink.getMode());
        assertTrue(!source.toString().contains("secret-value"));
        assertTrue(!sink.toString().contains("secret-value"));
    }
}
