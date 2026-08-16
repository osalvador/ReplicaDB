package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobDefinitionEnvResolverTest {

    private final JobDefinitionEnvResolver resolver = new JobDefinitionEnvResolver(
            Map.of("DB_HOST", "source-host", "DB_PORT", "5432")::get);

    @Test
    void leavesPlainValuesAndNullUnchanged() {
        assertEquals("jdbc:postgresql://source-host", resolver.resolve("jdbc:postgresql://source-host"));
        assertNull(resolver.resolve(null));
    }

    @Test
    void resolvesMultipleEnvironmentReferences() {
        assertEquals("jdbc:postgresql://source-host:5432/database",
                resolver.resolve("jdbc:postgresql://${env:DB_HOST}:${env:DB_PORT}/database"));
    }

    @Test
    void rejectsMissingEnvironmentVariableWithoutExposingTemplate() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve("jdbc:postgresql://${env:MISSING}/database"));

        assertEquals("Missing environment variable: MISSING", exception.getMessage());
    }

    @Test
    void rejectsSecretManagerReferences() {
        assertThrows(UnsupportedOperationException.class,
                () -> resolver.resolve("${secret:provider/path#key}"));
    }
}
