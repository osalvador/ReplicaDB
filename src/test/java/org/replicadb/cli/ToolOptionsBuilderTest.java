package org.replicadb.cli;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ToolOptionsBuilderTest {

    @Test
    void buildsAllManagedValuesInMemory() {
        Properties sourceParams = new Properties();
        sourceParams.setProperty("driver", "source-driver");
        Properties sinkParams = new Properties();
        sinkParams.setProperty("topic", "orders");
        AzureAuthenticationOptions sourceAuthentication = new AzureAuthenticationOptions();
        sourceAuthentication.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT);
        sourceAuthentication.setPrincipalId("source-principal");

        ToolOptions options = new ToolOptionsBuilder()
                .sourceConnect("jdbc:source/${literal}")
                .sourceUser("source-user")
                .sourcePassword("password-placeholder-${literal}")
                .sourceTable("source_table")
                .sourceColumns("id, value")
                .sourceWhere("id > 0")
                .sourceFileFormat("csv")
                .incrementalWatermarkColumn("updated_at")
                .incrementalWatermarkValue("42")
                .sinkConnect("jdbc:sink/${literal}")
                .sinkUser("sink-user")
                .sinkPassword("sink-password-placeholder")
                .sinkTable("sink_table")
                .sinkStagingTable("staging_table")
                .sinkStagingTableAlias("staging_alias")
                .sinkStagingSchema("staging_schema")
                .sinkColumns("id, value")
                .sinkFileFormat("csv")
                .sinkDisableEscape(true)
                .sinkDisableIndex(true)
                .sinkDisableTruncate(true)
                .sinkAutoCreate(true)
                .sinkAnalyze(true)
                .jobs(2)
                .fetchSize(250)
                .bandwidthThrottling(1024)
                .verbose(true)
                .quotedIdentifiers(true)
                .mode(ReplicationMode.INCREMENTAL.getModeText())
                .sourceConnectionParams(sourceParams)
                .sinkConnectionParams(sinkParams)
                .sourceAuthentication(sourceAuthentication)
                .sentryDsn("dsn-placeholder")
                .build();

        assertEquals("jdbc:source/${literal}", options.getSourceConnect());
        assertEquals("source-user", options.getSourceUser());
        assertEquals("password-placeholder-${literal}", options.getSourcePassword());
        assertEquals("source_table", options.getSourceTable());
        assertEquals("id, value", options.getSourceColumns());
        assertEquals("id > 0", options.getSourceWhere());
        assertEquals("csv", options.getSourceFileFormat());
        assertEquals("updated_at", options.getIncrementalWatermarkColumn());
        assertEquals("42", options.getIncrementalWatermarkValue());
        assertEquals("jdbc:sink/${literal}", options.getSinkConnect());
        assertEquals("sink-user", options.getSinkUser());
        assertEquals("sink-password-placeholder", options.getSinkPassword());
        assertEquals("sink_table", options.getSinkTable());
        assertEquals("staging_table", options.getSinkStagingTable());
        assertEquals("staging_alias", options.getSinkStagingTableAlias());
        assertEquals("staging_schema", options.getSinkStagingSchema());
        assertEquals("id, value", options.getSinkColumns());
        assertEquals("csv", options.getSinkFileFormat());
        assertEquals(true, options.isSinkDisableEscape());
        assertEquals(true, options.getSinkDisableIndex());
        assertEquals(true, options.isSinkDisableTruncate());
        assertEquals(true, options.isSinkAutoCreate());
        assertEquals(true, options.getSinkAnalyze());
        assertEquals(2, options.getJobs());
        assertEquals(250, options.getFetchSize());
        assertEquals(1024, options.getBandwidthThrottling());
        assertEquals(Level.DEBUG, options.getVerboseLevel());
        assertEquals(true, options.getQuotedIdentifiers());
        assertEquals(ReplicationMode.INCREMENTAL.getModeText(), options.getMode());
        assertEquals("source-driver", options.getSourceConnectionParams().getProperty("driver"));
        assertEquals("orders", options.getSinkConnectionParams().getProperty("topic"));
        assertEquals(AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT,
                options.getSourceAuthentication().getMode());
        assertEquals("source-principal", options.getSourceAuthentication().getPrincipalId());
        assertEquals("dsn-placeholder", options.getSentryDsn());
        assertNull(options.getOptionsFile());
        assertFalse(options.hasReplicationTables());
    }

    @Test
    void copiesMutableInputs() {
        Properties sourceParams = new Properties();
        sourceParams.setProperty("driver", "original");
        AzureAuthenticationOptions authentication = new AzureAuthenticationOptions();
        authentication.setMode(AzureAuthenticationMode.ACTIVE_DIRECTORY_DEFAULT);
        ToolOptionsBuilder builder = new ToolOptionsBuilder()
                .sourceConnect("jdbc:source")
                .sinkConnect("jdbc:sink")
                .sourceConnectionParams(sourceParams)
                .sourceAuthentication(authentication);

        sourceParams.setProperty("driver", "changed");
        authentication.setPrincipalId("changed");

        ToolOptions options = builder.build();

        assertEquals("original", options.getSourceConnectionParams().getProperty("driver"));
        assertNull(options.getSourceAuthentication().getPrincipalId());
        assertNotSame(sourceParams, options.getSourceConnectionParams());
        assertNotSame(authentication, options.getSourceAuthentication());
    }

    @Test
    void appliesDefaultsWithoutOptionsFile() {
        ToolOptions options = new ToolOptionsBuilder()
                .sourceConnect("jdbc:source")
                .sinkConnect("jdbc:sink")
                .build();

        assertEquals(4, options.getJobs());
        assertEquals(100, options.getFetchSize());
        assertEquals(0, options.getBandwidthThrottling());
        assertEquals(ReplicationMode.COMPLETE.getModeText(), options.getMode());
        assertEquals(Level.INFO, options.getVerboseLevel());
        assertNull(options.getOptionsFile());
    }

    @Test
    void rejectsInvalidManagedValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptionsBuilder().sinkConnect("jdbc:sink").build());
        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptionsBuilder()
                        .sourceConnect("jdbc:source")
                        .sinkConnect("jdbc:sink")
                        .jobs(0)
                        .build());
        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptionsBuilder()
                        .sourceConnect("jdbc:source")
                        .sinkConnect("jdbc:sink")
                        .incrementalWatermarkValue("42")
                        .build());
        assertThrows(IllegalArgumentException.class,
                () -> new ToolOptionsBuilder()
                        .sourceConnect("jdbc:source")
                        .sinkConnect("jdbc:sink")
                        .incrementalWatermarkColumn("updated_at")
                        .mode(ReplicationMode.COMPLETE.getModeText())
                        .build());
    }
}
