package org.replicadb.config;

import io.sentry.Breadcrumb;
import io.sentry.SentryEvent;
import io.sentry.protocol.Message;
import io.sentry.protocol.SentryException;
import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CredentialRedactorTest {

    @Test
    void redactsSecretsIdentityValuesAndJdbcCredentials() {
        Properties properties = new Properties();
        properties.setProperty("password", "password-secret");
        properties.setProperty("accessToken", "access-token-secret");
        properties.setProperty("clientSecret", "client-secret");
        properties.setProperty("user", "user@example.com");
        properties.setProperty("authentication", "ActiveDirectoryDefault");
        properties.setProperty("source.password", "prefixed-password-secret");
        properties.setProperty("source.auth.principal.id", "principal-secret");
        properties.setProperty("source.connect", "jdbc:sqlserver://host;user=alice;password=url-secret");

        Properties redacted = CredentialRedactor.redactProperties(properties);

        assertEquals(CredentialRedactor.REDACTED_VALUE, redacted.getProperty("password"));
        assertEquals(CredentialRedactor.REDACTED_VALUE, redacted.getProperty("accessToken"));
        assertEquals(CredentialRedactor.REDACTED_VALUE, redacted.getProperty("clientSecret"));
        assertEquals(CredentialRedactor.REDACTED_VALUE, redacted.getProperty("user"));
        assertEquals(CredentialRedactor.REDACTED_VALUE, redacted.getProperty("source.password"));
        assertEquals(CredentialRedactor.REDACTED_VALUE, redacted.getProperty("source.auth.principal.id"));
        assertEquals("ActiveDirectoryDefault", redacted.getProperty("authentication"));
        assertFalse(redacted.getProperty("source.connect").contains("url-secret"));
        assertFalse(redacted.getProperty("source.connect").contains("alice"));
    }

    @Test
    void redactsUrlsAndExceptionMessages() {
        String url = "jdbc:postgresql://alice:url-secret@host/db?user=alice&password=url-secret";
        String redacted = CredentialRedactor.redactConnectionString(url);

        assertFalse(redacted.contains("url-secret"));
        assertFalse(redacted.contains("alice"));

        Throwable exception = CredentialRedactor.redactThrowable(
                new IllegalStateException("authentication failed password=url-secret"));
        assertNotNull(exception.getMessage());
        assertFalse(exception.getMessage().contains("url-secret"));
    }

    @Test
    void redactsEnvironmentPlaceholdersAndPemBlocks() {
        String redacted = CredentialRedactor.redactMessage(
                "${env:MASTER_KEY} -----BEGIN PRIVATE KEY-----key-----END PRIVATE KEY-----");

        assertFalse(redacted.contains("MASTER_KEY"));
        assertFalse(redacted.contains("PRIVATE KEY"));
    }

    @Test
    void redactsSentryMessagesContextsExceptionsAndBreadcrumbs() {
        SentryEvent event = new SentryEvent(new IllegalStateException("password=event-secret"));
        Message message = new Message();
        message.setMessage("accessToken=event-token");
        message.setFormatted("clientSecret=event-client-secret");
        message.setParams(List.of("user=event-user"));
        event.setMessage(message);
        event.setTag("source.connect", "jdbc:sqlserver://host;password=tag-secret");

        Properties contextProperties = new Properties();
        contextProperties.setProperty("password", "context-secret");
        event.getContexts().put("connection", contextProperties);

        SentryException sentryException = new SentryException();
        sentryException.setValue("AADSecurePrincipalSecret=exception-secret");
        event.setExceptions(List.of(sentryException));

        Breadcrumb breadcrumb = new Breadcrumb();
        breadcrumb.setMessage("secretKey=breadcrumb-secret");
        breadcrumb.setData("accessToken", "breadcrumb-token");
        event.setBreadcrumbs(List.of(breadcrumb));

        CredentialRedactor.redactEvent(event);

        assertFalse(event.getMessage().getMessage().contains("event-token"));
        assertFalse(event.getMessage().getFormatted().contains("event-client-secret"));
        assertFalse(event.getMessage().getParams().get(0).contains("event-user"));
        assertFalse(event.getTag("source.connect").contains("tag-secret"));
        assertEquals(CredentialRedactor.REDACTED_VALUE,
                ((Properties) event.getContexts().get("connection")).getProperty("password"));
        assertFalse(event.getExceptions().get(0).getValue().contains("exception-secret"));
        assertFalse(event.getBreadcrumbs().get(0).getMessage().contains("breadcrumb-secret"));
        assertEquals(CredentialRedactor.REDACTED_VALUE,
                event.getBreadcrumbs().get(0).getData("accessToken"));
        assertFalse(event.getThrowable().getMessage().contains("event-secret"));
    }

    @Test
    void ToolOptionsDebugOutputDoesNotContainConfiguredSentinels() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlserver://host;user=alice;password=url-secret",
                "--source-user", "user@example.com",
                "--source-password", "password-secret",
                "--sink-connect", "jdbc:sqlserver://sink"
        });
        Properties connectionParams = new Properties();
        connectionParams.setProperty("accessToken", "access-token-secret");
        options.setSourceConnectionParams(connectionParams);

        String output = options.toString();

        assertTrue(output.contains(CredentialRedactor.REDACTED_VALUE));
        assertFalse(output.contains("url-secret"));
        assertFalse(output.contains("user@example.com"));
        assertFalse(output.contains("access-token-secret"));
    }
}
