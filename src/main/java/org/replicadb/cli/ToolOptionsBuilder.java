package org.replicadb.cli;

import org.apache.logging.log4j.Level;

import java.util.Objects;
import java.util.Properties;

/**
 * Builds execution options for callers that already have structured configuration.
 * The command-line parser and options-file contract remain separate entry points.
 */
public final class ToolOptionsBuilder {

    private String sourceConnect;
    private String sourceUser;
    private String sourcePassword;
    private String sourceTable;
    private String sourceColumns;
    private String sourceWhere;
    private String sourceQuery;
    private String sourceFileFormat;
    private String incrementalWatermarkColumn;
    private String incrementalWatermarkValue;

    private String sinkConnect;
    private String sinkUser;
    private String sinkPassword;
    private String sinkTable;
    private String sinkStagingTable;
    private String sinkStagingTableAlias;
    private String sinkStagingSchema;
    private String sinkColumns;
    private String sinkFileFormat;
    private boolean sinkDisableEscape;
    private boolean sinkDisableIndex;
    private boolean sinkDisableTruncate;
    private boolean sinkAutoCreate;
    private boolean sinkAnalyze;

    private int jobs = 4;
    private int fetchSize = 100;
    private int bandwidthThrottling;
    private Level verboseLevel = Level.INFO;
    private boolean quotedIdentifiers;
    private String mode = ReplicationMode.COMPLETE.getModeText();

    private Properties sourceConnectionParams = new Properties();
    private Properties sinkConnectionParams = new Properties();
    private AzureAuthenticationOptions sourceAuthentication = new AzureAuthenticationOptions();
    private AzureAuthenticationOptions sinkAuthentication = new AzureAuthenticationOptions();
    private String sentryDsn;

    public ToolOptionsBuilder sourceConnect(String value) {
        sourceConnect = value;
        return this;
    }

    public ToolOptionsBuilder sourceUser(String value) {
        sourceUser = value;
        return this;
    }

    public ToolOptionsBuilder sourcePassword(String value) {
        sourcePassword = value;
        return this;
    }

    public ToolOptionsBuilder sourceTable(String value) {
        sourceTable = value;
        return this;
    }

    public ToolOptionsBuilder sourceColumns(String value) {
        sourceColumns = value;
        return this;
    }

    public ToolOptionsBuilder sourceWhere(String value) {
        sourceWhere = value;
        return this;
    }

    public ToolOptionsBuilder sourceQuery(String value) {
        sourceQuery = value;
        return this;
    }

    public ToolOptionsBuilder sourceFileFormat(String value) {
        sourceFileFormat = value;
        return this;
    }

    public ToolOptionsBuilder incrementalWatermarkColumn(String value) {
        incrementalWatermarkColumn = value;
        return this;
    }

    public ToolOptionsBuilder incrementalWatermarkValue(String value) {
        incrementalWatermarkValue = value;
        return this;
    }

    public ToolOptionsBuilder sinkConnect(String value) {
        sinkConnect = value;
        return this;
    }

    public ToolOptionsBuilder sinkUser(String value) {
        sinkUser = value;
        return this;
    }

    public ToolOptionsBuilder sinkPassword(String value) {
        sinkPassword = value;
        return this;
    }

    public ToolOptionsBuilder sinkTable(String value) {
        sinkTable = value;
        return this;
    }

    public ToolOptionsBuilder sinkStagingTable(String value) {
        sinkStagingTable = value;
        return this;
    }

    public ToolOptionsBuilder sinkStagingTableAlias(String value) {
        sinkStagingTableAlias = value;
        return this;
    }

    public ToolOptionsBuilder sinkStagingSchema(String value) {
        sinkStagingSchema = value;
        return this;
    }

    public ToolOptionsBuilder sinkColumns(String value) {
        sinkColumns = value;
        return this;
    }

    public ToolOptionsBuilder sinkFileFormat(String value) {
        sinkFileFormat = value;
        return this;
    }

    public ToolOptionsBuilder sinkDisableEscape(boolean value) {
        sinkDisableEscape = value;
        return this;
    }

    public ToolOptionsBuilder sinkDisableIndex(boolean value) {
        sinkDisableIndex = value;
        return this;
    }

    public ToolOptionsBuilder sinkDisableTruncate(boolean value) {
        sinkDisableTruncate = value;
        return this;
    }

    public ToolOptionsBuilder sinkAutoCreate(boolean value) {
        sinkAutoCreate = value;
        return this;
    }

    public ToolOptionsBuilder sinkAnalyze(boolean value) {
        sinkAnalyze = value;
        return this;
    }

    public ToolOptionsBuilder jobs(int value) {
        jobs = value;
        return this;
    }

    public ToolOptionsBuilder fetchSize(int value) {
        fetchSize = value;
        return this;
    }

    public ToolOptionsBuilder bandwidthThrottling(int value) {
        bandwidthThrottling = value;
        return this;
    }

    public ToolOptionsBuilder verbose(boolean value) {
        verboseLevel = value ? Level.DEBUG : Level.INFO;
        return this;
    }

    public ToolOptionsBuilder verboseLevel(Level value) {
        verboseLevel = Objects.requireNonNull(value, "verboseLevel must not be null");
        return this;
    }

    public ToolOptionsBuilder quotedIdentifiers(boolean value) {
        quotedIdentifiers = value;
        return this;
    }

    public ToolOptionsBuilder mode(String value) {
        mode = value;
        return this;
    }

    public ToolOptionsBuilder sourceConnectionParams(Properties value) {
        sourceConnectionParams = copyProperties(value);
        return this;
    }

    public ToolOptionsBuilder sinkConnectionParams(Properties value) {
        sinkConnectionParams = copyProperties(value);
        return this;
    }

    public ToolOptionsBuilder sourceAuthentication(AzureAuthenticationOptions value) {
        sourceAuthentication = copyAuthentication(value);
        return this;
    }

    public ToolOptionsBuilder sinkAuthentication(AzureAuthenticationOptions value) {
        sinkAuthentication = copyAuthentication(value);
        return this;
    }

    public ToolOptionsBuilder sentryDsn(String value) {
        sentryDsn = value;
        return this;
    }

    public ToolOptions build() {
        validate();

        ToolOptions options = new ToolOptions();
        options.setSourceConnect(sourceConnect);
        options.setSourceUser(sourceUser);
        options.setSourcePassword(sourcePassword);
        options.setSourceTable(sourceTable);
        options.setSourceColumns(sourceColumns);
        options.setSourceWhere(sourceWhere);
        options.setSourceQuery(sourceQuery);
        options.setSourceFileFormat(sourceFileFormat);
        options.setIncrementalWatermarkColumn(incrementalWatermarkColumn);
        options.setIncrementalWatermarkValue(incrementalWatermarkValue);
        options.setSinkConnect(sinkConnect);
        options.setSinkUser(sinkUser);
        options.setSinkPassword(sinkPassword);
        options.setSinkTable(sinkTable);
        options.setSinkStagingTable(sinkStagingTable);
        options.setSinkStagingTableAlias(sinkStagingTableAlias);
        options.setSinkStagingSchema(sinkStagingSchema);
        options.setSinkColumns(sinkColumns);
        options.setSinkFileFormat(sinkFileFormat);
        options.setSinkDisableEscape(sinkDisableEscape);
        options.setSinkDisableIndex(sinkDisableIndex);
        options.setSinkDisableTruncate(sinkDisableTruncate);
        options.setSinkAutoCreate(sinkAutoCreate);
        options.setSinkAnalyze(sinkAnalyze);
        options.setJobs(Integer.toString(jobs));
        options.setFetchSize(Integer.toString(fetchSize));
        options.setBandwidthThrottling(Integer.toString(bandwidthThrottling));
        options.setVerboseLevel(verboseLevel);
        options.setQuotedIdentifiers(quotedIdentifiers);
        options.setMode(mode);
        options.setSourceConnectionParams(copyProperties(sourceConnectionParams));
        options.setSinkConnectionParams(copyProperties(sinkConnectionParams));
        options.setSourceAuthentication(copyAuthentication(sourceAuthentication));
        options.setSinkAuthentication(copyAuthentication(sinkAuthentication));
        options.setSentryDsn(sentryDsn);
        return options;
    }

    private void validate() {
        requireNonBlank("sourceConnect", sourceConnect);
        requireNonBlank("sinkConnect", sinkConnect);
        if (jobs < 1) {
            throw new IllegalArgumentException("jobs must be at least 1");
        }
        if (fetchSize < 1) {
            throw new IllegalArgumentException("fetchSize must be at least 1");
        }
        if (bandwidthThrottling < 0) {
            throw new IllegalArgumentException("bandwidthThrottling must not be negative");
        }
        if (hasValue(incrementalWatermarkValue) && !hasValue(incrementalWatermarkColumn)) {
            throw new IllegalArgumentException(
                    "incremental-watermark-value cannot be used without incremental-watermark-column.");
        }
        if (hasValue(incrementalWatermarkColumn)
                && !ReplicationMode.INCREMENTAL.getModeText().equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException(
                    "incremental-watermark-column is only supported with incremental mode.");
        }
    }

    private static void requireNonBlank(String fieldName, String value) {
        if (!hasValue(value)) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static boolean hasValue(String value) {
        return value != null && !value.isBlank();
    }

    private static Properties copyProperties(Properties value) {
        Properties copy = new Properties();
        if (value != null) {
            copy.putAll(value);
        }
        return copy;
    }

    private static AzureAuthenticationOptions copyAuthentication(AzureAuthenticationOptions value) {
        return value == null ? new AzureAuthenticationOptions() : new AzureAuthenticationOptions(value);
    }
}
