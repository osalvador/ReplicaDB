package org.replicadb.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ToolOptionsAzureAuthenticationTest {

    @Test
    void parsesSourceAndSinkAuthenticationFromCommandLine() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlserver://source",
                "--sink-connect", "jdbc:sqlserver://sink",
                "--source-auth-mode", "ActiveDirectoryInteractive",
                "--source-auth-login-hint", "source@example.com",
                "--sink-auth-mode", "ActiveDirectoryManagedIdentity",
                "--sink-auth-principal-id", "managed-identity-client"
        });

        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE,
                options.getSourceAuthentication().getMode());
        assertEquals("source@example.com", options.getSourceAuthentication().getLoginHint());
        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_MANAGED_IDENTITY,
                options.getSinkAuthentication().getMode());
        assertEquals("managed-identity-client", options.getSinkAuthentication().getPrincipalId());
    }

    @Test
    void optionsFileValuesAreOverriddenByNonEmptyCommandLineValues(@TempDir Path tempDir) throws Exception {
        Path optionsFile = tempDir.resolve("azure.properties");
        try (FileWriter writer = new FileWriter(optionsFile.toFile())) {
            writer.write("source.connect=jdbc:sqlserver://source\n");
            writer.write("sink.connect=jdbc:sqlserver://sink\n");
            writer.write("source.auth.mode=ActiveDirectoryDefault\n");
            writer.write("source.auth.login.hint=file@example.com\n");
            writer.write("sink.auth.mode=ActiveDirectoryManagedIdentity\n");
        }

        ToolOptions options = new ToolOptions(new String[]{
                "--options-file", optionsFile.toString(),
                "--source-auth-mode", "ActiveDirectoryInteractive",
                "--source-auth-login-hint", "cli@example.com"
        });

        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE,
                options.getSourceAuthentication().getMode());
        assertEquals("cli@example.com", options.getSourceAuthentication().getLoginHint());
        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_MANAGED_IDENTITY,
                options.getSinkAuthentication().getMode());
    }

    @Test
    void expandsEnvironmentVariablesInAuthenticationProperties(@TempDir Path tempDir) throws Exception {
        String home = System.getenv("HOME");
        assertTrue(home != null && !home.isBlank(), "HOME must be available for this test");

        Path optionsFile = tempDir.resolve("azure.properties");
        try (FileWriter writer = new FileWriter(optionsFile.toFile())) {
            writer.write("source.connect=jdbc:sqlserver://source\n");
            writer.write("sink.connect=jdbc:sqlserver://sink\n");
            writer.write("source.auth.mode=ActiveDirectoryServicePrincipalCertificate\n");
            writer.write("source.auth.principal.id=client-id\n");
            writer.write("source.auth.client.certificate=${HOME}/client.pfx\n");
        }

        ToolOptions options = new ToolOptions(new String[]{"--options-file", optionsFile.toString()});

        assertEquals(home + "/client.pfx", options.getSourceAuthentication().getClientCertificate());
    }

    @Test
    void helpListsAuthenticationOptionsWithoutCredentials() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
        try {
            new ToolOptions(new String[]{"--help"});
        } finally {
            System.setOut(originalOut);
        }

        String help = output.toString(StandardCharsets.UTF_8);
        assertTrue(help.contains("--source-auth-mode"));
        assertTrue(help.contains("--sink-auth-mode"));
        assertTrue(!help.contains("client-secret"));
    }
}
