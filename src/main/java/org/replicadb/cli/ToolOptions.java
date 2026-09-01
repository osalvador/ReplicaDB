package org.replicadb.cli;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.execution.ReplicationExecutionContext;
import org.replicadb.manager.util.ColumnDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class ToolOptions {

    private static final Logger LOG = LogManager.getLogger(ToolOptions.class.getName());
    private static final int DEFAULT_JOBS = 4;
    private static final int DEFAULT_FETCH_SIZE = 100;
    private static final String DEFAULT_MODE = ReplicationMode.COMPLETE.getModeText();

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
    private Boolean sinkDisableEscape = false;
    private Boolean sinkDisableIndex = false;
    private Boolean sinkDisableTruncate = false;
    private Boolean sinkAutoCreate = false;
    private Boolean sinkAnalyze = false;


    private int jobs = DEFAULT_JOBS;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int bandwidthThrottling = 0;
    private Boolean help = false;
    private Boolean version = false;
    private Level verboseLevel = Level.INFO;
    private Boolean quotedIdentifiers = false;
    private String optionsFile;

    private String mode = DEFAULT_MODE;

    private Properties sourceConnectionParams;
    private Properties sinkConnectionParams;
    private AzureAuthenticationOptions sourceAuthentication = new AzureAuthenticationOptions();
    private AzureAuthenticationOptions sinkAuthentication = new AzureAuthenticationOptions();
    private String sentryDsn;

    private List<ColumnDescriptor> sourceColumnDescriptors;
    private String[] sourcePrimaryKeys;
    private List<ReplicationTable> replicationTables = List.of();
    private final ReplicationExecutionContext executionContext = new ReplicationExecutionContext();

    private Options options;

    public ToolOptions(String[] args) throws ParseException, IOException {
        checkOptions(args);
    }

    ToolOptions() {
    }

    public ToolOptions forReplicationTable(ReplicationTable replicationTable) {
        Objects.requireNonNull(replicationTable, "replicationTable must not be null");

        ToolOptions copy = new ToolOptions();
        copy.sourceConnect = sourceConnect;
        copy.sourceUser = sourceUser;
        copy.sourcePassword = sourcePassword;
        copy.sourceTable = replicationTable.sourceTable();
        copy.sourceColumns = sourceColumns;
        copy.sourceWhere = sourceWhere;
        copy.sourceQuery = sourceQuery;
        copy.sourceFileFormat = sourceFileFormat;
        // incrementalWatermarkColumn/incrementalWatermarkValue are never copied: validateIncrementalWatermarkOptions()
        // rejects combining them with replication.table.* entries, so a replication-table copy never needs them.
        copy.sinkConnect = sinkConnect;
        copy.sinkUser = sinkUser;
        copy.sinkPassword = sinkPassword;
        copy.sinkTable = replicationTable.sinkTable();
        copy.sinkStagingTable = sinkStagingTable;
        copy.sinkStagingTableAlias = sinkStagingTableAlias;
        copy.sinkStagingSchema = sinkStagingSchema;
        copy.sinkColumns = sinkColumns;
        copy.sinkFileFormat = sinkFileFormat;
        copy.sinkDisableEscape = sinkDisableEscape;
        copy.sinkDisableIndex = sinkDisableIndex;
        copy.sinkDisableTruncate = sinkDisableTruncate;
        copy.sinkAutoCreate = sinkAutoCreate;
        copy.sinkAnalyze = sinkAnalyze;
        copy.jobs = jobs;
        copy.fetchSize = fetchSize;
        copy.bandwidthThrottling = bandwidthThrottling;
        copy.help = help;
        copy.version = version;
        copy.verboseLevel = verboseLevel;
        copy.quotedIdentifiers = quotedIdentifiers;
        copy.optionsFile = optionsFile;
        copy.mode = mode;
        copy.sourceConnectionParams = copyProperties(sourceConnectionParams);
        copy.sinkConnectionParams = copyProperties(sinkConnectionParams);
        copy.sourceAuthentication = new AzureAuthenticationOptions(sourceAuthentication);
        copy.sinkAuthentication = new AzureAuthenticationOptions(sinkAuthentication);
        copy.sentryDsn = sentryDsn;
        copy.sourceColumnDescriptors = sourceColumnDescriptors == null
                ? null
                : List.copyOf(sourceColumnDescriptors);
        copy.sourcePrimaryKeys = sourcePrimaryKeys == null
                ? null
                : Arrays.copyOf(sourcePrimaryKeys, sourcePrimaryKeys.length);
        copy.replicationTables = List.of();
        copy.options = options;
        return copy;
    }

    private static Properties copyProperties(Properties properties) {
        if (properties == null) {
            return null;
        }

        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    private void checkOptions(String[] args) throws ParseException, IOException {

        this.options = new Options();

        // Source Options
        options.addOption(
                Option.builder()
                        .longOpt("source-connect")
                        .desc("Source database JDBC connect string")
                        .hasArg()
                        //.required()
                        .argName("jdbc-uri")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-user")
                        .desc("Source database authentication username")
                        .hasArg()
                        .argName("username")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-password")
                        .desc("Source database authentication password")
                        .hasArg()
                        .argName("password")
                        .build()
        );

                options.addOption(
                    Option.builder()
                        .longOpt("source-auth-mode")
                        .desc("Source Microsoft Entra authentication mode")
                        .hasArg()
                        .argName("auth-mode")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("source-auth-principal-id")
                        .desc("Source Microsoft Entra principal or managed identity client ID")
                        .hasArg()
                        .argName("principal-id")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("source-auth-login-hint")
                        .desc("Source Microsoft Entra interactive login hint")
                        .hasArg()
                        .argName("login-hint")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("source-auth-client-certificate")
                        .desc("Source Microsoft Entra service principal certificate path")
                        .hasArg()
                        .argName("certificate-path")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("source-auth-client-key")
                        .desc("Source Microsoft Entra service principal private key path")
                        .hasArg()
                        .argName("key-path")
                        .build()
                );

        options.addOption(
                Option.builder()
                        .longOpt("source-table")
                        .desc("Source database table to read")
                        .hasArg()
                        .argName("table-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-columns")
                        .desc("Source database table columns to be extracted")
                        .hasArg()
                        .argName("col,col,col...")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-where")
                        .desc("Source database WHERE clause to use during extraction")
                        .hasArg()
                        .argName("where clause")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("incremental-watermark-column")
                        .desc("Source database column to use as the incremental watermark. Only valid with --mode incremental.")
                        .hasArg()
                        .argName("column-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("incremental-watermark-value")
                        .desc("Last committed watermark value; rows with a watermark column value greater than this are replicated. Absent on the first run replicates everything.")
                        .hasArg()
                        .argName("value")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-query")
                        .desc("SQL statement to be executed in the source database")
                        .hasArg()
                        .argName("statement")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-file-format")
                        .desc("Source file format. The allowed values are csv, json, avro, parquet, orc")
                        .hasArg()
                        .argName("file format")
                        .build()
        );

        // Sink Options
        options.addOption(
                Option.builder()
                        .longOpt("sink-connect")
                        .desc("Sink database JDBC connect string")
                        .hasArg()
                        //.required()
                        .argName("jdbc-uri")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-user")
                        .desc("Sink database authentication username")
                        .hasArg()
                        .argName("username")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-password")
                        .desc("Sink database authentication password")
                        .hasArg()
                        .argName("password")
                        .build()
        );

                options.addOption(
                    Option.builder()
                        .longOpt("sink-auth-mode")
                        .desc("Sink Microsoft Entra authentication mode")
                        .hasArg()
                        .argName("auth-mode")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("sink-auth-principal-id")
                        .desc("Sink Microsoft Entra principal or managed identity client ID")
                        .hasArg()
                        .argName("principal-id")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("sink-auth-login-hint")
                        .desc("Sink Microsoft Entra interactive login hint")
                        .hasArg()
                        .argName("login-hint")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("sink-auth-client-certificate")
                        .desc("Sink Microsoft Entra service principal certificate path")
                        .hasArg()
                        .argName("certificate-path")
                        .build()
                );

                options.addOption(
                    Option.builder()
                        .longOpt("sink-auth-client-key")
                        .desc("Sink Microsoft Entra service principal private key path")
                        .hasArg()
                        .argName("key-path")
                        .build()
                );

        options.addOption(
                Option.builder()
                        .longOpt("sink-table")
                        .desc("Sink database table to populate")
                        .hasArg()
                        .argName("table-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-columns")
                        .desc("Sink database table columns to be populated")
                        .hasArg()
                        .argName("col,col,col...")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-disable-escape")
                        .desc("Escape strings before populating to the table of the sink database.")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("sink-disable-index")
                        .desc("Disable sink database table indexes before populate.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-disable-truncate")
                        .desc("Disable the truncation of the sink database table before populate.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-auto-create")
                        .desc("Automatically create the sink table if it does not exist.")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("sink-analyze")
                        .desc("Analyze sink database table after populate.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-staging-table")
                        .desc("Qualified name of the sink staging table. The table must exist in the sink database.")
                        .hasArg()
                        .argName("staging-table-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-staging-table-alias")
                        .desc("Alias name for the sink staging table. The table must exist in the sink database.")
                        .hasArg()
                        .argName("staging-table-name-alias")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-staging-schema")
                        .desc("Scheme name on the sink database, with right permissions for creating staging tables.")
                        .hasArg()
                        .argName("staging-schema")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-file-format")
                        .desc("Sink file format. The allowed values are csv, json, avro, parquet, orc")
                        .hasArg()
                        .argName("file format")
                        .build()
        );

        // Other Options
        options.addOption(
                Option.builder()
                        .longOpt("options-file")
                        .desc("Options file path location")
                        .hasArg()
                        .argName("file-path")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("mode")
                        .desc("Specifies the replication mode. The allowed values are complete, complete-atomic or incremental.")
                        //.required()
                        .hasArg()
                        .argName("mode")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("fetch-size")
                        .desc("Number of entries to read from database at once.")
                        .hasArg()
                        .argName("fetch-size")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("version")
                        .desc("Show implementation version and exit.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("bandwidth-throttling")
                        .desc("Adds a bandwidth cap for the replication in KB/sec.")
                        .hasArg()
                        .argName("KB/s")
                        .build()
        );


        Option helpOpt = new Option("h", "help", false, "Print this help screen");
        options.addOption(helpOpt);

        Option jobsOpt = new Option("j", "jobs", true, "Use n jobs to replicate in parallel. Default 4");
        jobsOpt.setArgName("n");
        options.addOption(jobsOpt);

        Option verboseOpt = new Option("v", "verbose", false, "Print more information while working");
        options.addOption(verboseOpt);

        options.addOption(
                Option.builder()
                        .longOpt("quoted-identifiers")
                        .desc("Should all database identifiers be quoted.")
                        .build()
        );


        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // If help argument is not passed is not necessary test the rest of arguments
        if (existsHelpArgument(args)) {
            printHelp();
            this.setHelp(true);
        } else if (existsVersionArgument(args)) {
            this.setVersion(true);
        } else {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            // check for optionsFile
            setOptionsFile(line.getOptionValue("options-file"));
            if (this.optionsFile != null && !this.optionsFile.isEmpty()) {
                loadOptionsFile();
            }

            //get & set Options
            if (line.hasOption("verbose")) handleVerboseLevelArgument(line.getOptionValue("verbose"));
            if (line.hasOption("sink-disable-index")) setSinkDisableIndexNotNull(true);
            if (line.hasOption("sink-disable-escape")) setSinkDisableEscapeNotNull(true);
            if (line.hasOption("sink-disable-truncate")) setSinkDisableTruncateNotNull(true);
            if (line.hasOption("sink-auto-create")) setSinkAutoCreateNotNull(true);
            if (line.hasOption("sink-analyze")) setSinkAnalyzeNotNull(true);
            if (line.hasOption("quoted-identifiers")) setQuotedIdentifiers(true);

            setModeNotNull(line.getOptionValue("mode"));
            setSinkColumnsNotNull(line.getOptionValue("sink-columns"));
            setSinkConnectNotNull(line.getOptionValue("sink-connect"));
            setHelp(line.hasOption("help"));
            setSinkPasswordNotNull(line.getOptionValue("sink-password"));
            setSinkAuthModeNotNull(line.getOptionValue("sink-auth-mode"));
            setSinkAuthPrincipalIdNotNull(line.getOptionValue("sink-auth-principal-id"));
            setSinkAuthLoginHintNotNull(line.getOptionValue("sink-auth-login-hint"));
            setSinkAuthClientCertificateNotNull(line.getOptionValue("sink-auth-client-certificate"));
            setSinkAuthClientKeyNotNull(line.getOptionValue("sink-auth-client-key"));
            setSinkTableNotNull(line.getOptionValue("sink-table"));
            setSinkUserNotNull(line.getOptionValue("sink-user"));
            setSourceColumnsNotNull(line.getOptionValue("source-columns"));
            setSourceConnectNotNull(line.getOptionValue("source-connect"));
            setSourcePasswordNotNull(line.getOptionValue("source-password"));
            setSourceAuthModeNotNull(line.getOptionValue("source-auth-mode"));
            setSourceAuthPrincipalIdNotNull(line.getOptionValue("source-auth-principal-id"));
            setSourceAuthLoginHintNotNull(line.getOptionValue("source-auth-login-hint"));
            setSourceAuthClientCertificateNotNull(line.getOptionValue("source-auth-client-certificate"));
            setSourceAuthClientKeyNotNull(line.getOptionValue("source-auth-client-key"));
            setSourceQueryNotNull(line.getOptionValue("source-query"));
            setSourceTableNotNull(line.getOptionValue("source-table"));
            setSourceUserNotNull(line.getOptionValue("source-user"));
            setSourceWhereNotNull(line.getOptionValue("source-where"));
            setIncrementalWatermarkColumnNotNull(line.getOptionValue("incremental-watermark-column"));
            setIncrementalWatermarkValueNotNull(line.getOptionValue("incremental-watermark-value"));
            setJobsNotNull(line.getOptionValue("jobs"));
            setFetchSizeNotNull(line.getOptionValue("fetch-size"));
            setBandwidthThrottlingNotNull(line.getOptionValue("bandwidth-throttling"));
            setSinkStagingSchemaNotNull(line.getOptionValue("sink-staging-schema"));
            setSinkStagingTableNotNull(line.getOptionValue("sink-staging-table"));
            setSinkStagingTableAliasNotNull(line.getOptionValue("sink-staging-table-alias"));
            setSourceFileFormatNotNull(line.getOptionValue("source-file-format"));
            setSinkFileFormatNotNull(line.getOptionValue("sink-file-format"));

            validateReplicationTableOptions(line);
            validateIncrementalWatermarkOptions();

            //Check for required values
            if (!checkRequiredValues()) throw new IllegalArgumentException("Missing any of the required parameters:" +
                    " source-connect=" + this.sourceConnect + " OR sink-connect=" + this.sinkConnect);
        }

    }

    private void handleVerboseLevelArgument(String verboseLevel) {
        if (verboseLevel == null || verboseLevel.isEmpty()) {
            setVerboseLevel(Level.INFO);
            return;
        } else if (Boolean.parseBoolean(verboseLevel)) {
            setVerboseLevel(Level.DEBUG);
            return;
        }

        try {
            Level argumentLevel = Level.valueOf(verboseLevel);
            setVerboseLevel(argumentLevel);
        } catch (IllegalArgumentException e) {
            setVerboseLevel(Level.INFO);
        }
    }

    private void printHelp() {
        String header = "\nArguments: \n";
        String footer = "\nPlease report issues at https://github.com/osalvador/ReplicaDB/issues";

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(140);
        formatter.printHelp("replicadb [OPTIONS]", header, this.options, footer, false);
    }

    private Boolean existsHelpArgument(String args[]) {
        //help argument is -h or --help
        for (int i = 0; i <= args.length - 1; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                return true;
            }
        }
        return false;
    }

    private Boolean existsVersionArgument(String args[]) {
        //help argument is -h or --help
        for (int i = 0; i <= args.length - 1; i++) {
            if (args[i].equals("--version")) {
                return true;
            }
        }
        return false;
    }

    private void validateReplicationTableOptions(CommandLine line) {
        if (!hasReplicationTables()) {
            return;
        }

        if (hasValue(sourceQuery)) {
            throw new IllegalArgumentException("source.query cannot be used with replication.table.* entries.");
        }
        if (hasValue(sourceTable) || hasValue(sinkTable)) {
            throw new IllegalArgumentException(
                    "source.table and sink.table cannot be used with replication.table.* entries.");
        }
        if (line.hasOption("source-table") || line.hasOption("sink-table")) {
            throw new IllegalArgumentException(
                    "--source-table and --sink-table cannot be used with replication.table.* entries.");
        }
        if (ReplicationMode.INCREMENTAL.getModeText().equals(mode)
                || ReplicationMode.COMPLETE_ATOMIC.getModeText().equals(mode)) {
            if (hasValue(sinkStagingTable) || hasValue(sinkStagingTableAlias)) {
                throw new IllegalArgumentException(
                        "Fixed sink staging tables are not supported with replication.table.* entries; "
                                + "configure sink.staging.schema instead.");
            }
        }
    }

    private void validateIncrementalWatermarkOptions() {
        if (hasValue(incrementalWatermarkValue) && !hasValue(incrementalWatermarkColumn)) {
            throw new IllegalArgumentException(
                    "incremental-watermark-value cannot be used without incremental-watermark-column.");
        }
        if (hasValue(incrementalWatermarkColumn)) {
            if (!ReplicationMode.INCREMENTAL.getModeText().equals(mode)) {
                throw new IllegalArgumentException(
                        "incremental-watermark-column is only supported with --mode incremental.");
            }
            if (hasReplicationTables()) {
                throw new IllegalArgumentException(
                        "incremental-watermark-column cannot be used with replication.table.* entries.");
            }
        }
    }

    public String getVersion() {
        return ToolOptions.class.getPackage().getImplementationVersion();
    }

    public void setVersion(Boolean version) {
        this.version = version;
    }

    public Boolean isVersion() {
        return version;
    }

    public Boolean checkRequiredValues() {


        if (this.mode == null) return false;
        if (this.sourceConnect == null) return false;
        if (this.sinkConnect == null) return false;

        return true;
    }

    private void loadOptionsFile() throws IOException {

        OptionsFile of = new OptionsFile(this.optionsFile);

        // set properties from options file to this ToolOptions
        Properties prop = of.getProperties();
        setSinkAnalyze(Boolean.parseBoolean(prop.getProperty("sink.analyze")));

        handleVerboseLevelArgument(prop.getProperty("verbose"));
        setMode(prop.getProperty("mode"));

        setSinkColumns(prop.getProperty("sink.columns"));
        setSinkConnect(prop.getProperty("sink.connect"));
        setSinkDisableIndex(Boolean.parseBoolean(prop.getProperty("sink.disable.index")));
        setSinkDisableEscape(Boolean.parseBoolean(prop.getProperty("sink.disable.escape")));
        setSinkDisableTruncate(Boolean.parseBoolean(prop.getProperty("sink.disable.truncate")));
        setSinkAutoCreate(Boolean.parseBoolean(prop.getProperty("sink.auto.create")));
        setSinkUser(prop.getProperty("sink.user"));
        setSinkPassword(prop.getProperty("sink.password"));
        setSinkAuthMode(prop.getProperty("sink.auth.mode"));
        setSinkAuthPrincipalId(prop.getProperty("sink.auth.principal.id"));
        setSinkAuthLoginHint(prop.getProperty("sink.auth.login.hint"));
        setSinkAuthClientCertificate(prop.getProperty("sink.auth.client.certificate"));
        setSinkAuthClientKey(prop.getProperty("sink.auth.client.key"));
        setSinkTable(prop.getProperty("sink.table"));
        setSinkStagingTable(prop.getProperty("sink.staging.table"));
        setSinkStagingTableAlias(prop.getProperty("sink.staging.table.alias"));
        setSinkStagingSchema(prop.getProperty("sink.staging.schema"));
        setSourceColumns(prop.getProperty("source.columns"));
        setSourceConnect(prop.getProperty("source.connect"));
        setSourcePassword(prop.getProperty("source.password"));
        setSourceAuthMode(prop.getProperty("source.auth.mode"));
        setSourceAuthPrincipalId(prop.getProperty("source.auth.principal.id"));
        setSourceAuthLoginHint(prop.getProperty("source.auth.login.hint"));
        setSourceAuthClientCertificate(prop.getProperty("source.auth.client.certificate"));
        setSourceAuthClientKey(prop.getProperty("source.auth.client.key"));
        setSourceQuery(prop.getProperty("source.query"));
        setSourceTable(prop.getProperty("source.table"));
        setSourceUser(prop.getProperty("source.user"));
        setSourceWhere(prop.getProperty("source.where"));
        setIncrementalWatermarkColumn(prop.getProperty("incremental.watermark.column"));
        setIncrementalWatermarkValue(prop.getProperty("incremental.watermark.value"));
        setJobs(prop.getProperty("jobs"));
        setFetchSize(prop.getProperty("fetch.size"));
        setBandwidthThrottling(prop.getProperty("bandwidth.throttling"));
        setQuotedIdentifiers(Boolean.parseBoolean(prop.getProperty("quoted.identifiers")));
        setSourceFileFormat(prop.getProperty("source.file.format"));
        setSinkFileFormat(prop.getProperty("sink.file.format"));
        setSentryDsn(prop.getProperty("sentry.dsn"));
        setReplicationTables(of.getReplicationTables());

        // Connection params
        setSinkConnectionParams(of.getSinkConnectionParams());
        setSourceConnectionParams(of.getSourceConnectionParams());
    }

    /*
     * Geeters & Setters
     */
    public String getSourceConnect() {
        return sourceConnect;
    }

    public ReplicationExecutionContext getExecutionContext() {
        return executionContext;
    }

    public void setSourceConnect(String sourceConnect) {
        this.sourceConnect = sourceConnect;
    }

    private void setSourceConnectNotNull(String sourceConnect) {
        if (sourceConnect != null && !sourceConnect.isEmpty())
            this.sourceConnect = sourceConnect;
    }

    public String getSourceUser() {
        return sourceUser;
    }

    public void setSourceUser(String sourceUser) {
        this.sourceUser = sourceUser;
    }

    public void setSourceUserNotNull(String sourceUser) {
        if (sourceUser != null && !sourceUser.isEmpty())
            this.sourceUser = sourceUser;
    }

    public void setSourceAuthMode(String mode) {
        sourceAuthentication.setMode(mode);
    }

    public void setSourceAuthModeNotNull(String mode) {
        if (mode != null && !mode.isEmpty())
            setSourceAuthMode(mode);
    }

    public void setSourceAuthPrincipalId(String principalId) {
        sourceAuthentication.setPrincipalId(principalId);
    }

    public void setSourceAuthPrincipalIdNotNull(String principalId) {
        if (principalId != null && !principalId.isEmpty())
            setSourceAuthPrincipalId(principalId);
    }

    public void setSourceAuthLoginHint(String loginHint) {
        sourceAuthentication.setLoginHint(loginHint);
    }

    public void setSourceAuthLoginHintNotNull(String loginHint) {
        if (loginHint != null && !loginHint.isEmpty())
            setSourceAuthLoginHint(loginHint);
    }

    public void setSourceAuthClientCertificate(String clientCertificate) {
        sourceAuthentication.setClientCertificate(clientCertificate);
    }

    public void setSourceAuthClientCertificateNotNull(String clientCertificate) {
        if (clientCertificate != null && !clientCertificate.isEmpty())
            setSourceAuthClientCertificate(clientCertificate);
    }

    public void setSourceAuthClientKey(String clientKey) {
        sourceAuthentication.setClientKey(clientKey);
    }

    public void setSourceAuthClientKeyNotNull(String clientKey) {
        if (clientKey != null && !clientKey.isEmpty())
            setSourceAuthClientKey(clientKey);
    }

    public String getSourcePassword() {
        return sourcePassword;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public void setSourcePasswordNotNull(String sourcePassword) {
        if (sourcePassword != null && !sourcePassword.isEmpty())
            this.sourcePassword = sourcePassword;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public void setSourceTableNotNull(String sourceTable) {
        if (sourceTable != null && !sourceTable.isEmpty())
            this.sourceTable = sourceTable;
    }

    public String getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(String sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public void setSourceColumnsNotNull(String sourceColumns) {
        if (sourceColumns != null && !sourceColumns.isEmpty())
            this.sourceColumns = sourceColumns;
    }

    public String getSourceWhere() {
        return sourceWhere;
    }

    public void setSourceWhere(String sourceWhere) {
        this.sourceWhere = sourceWhere;
    }

    public void setSourceWhereNotNull(String sourceWhere) {
        if (sourceWhere != null && !sourceWhere.isEmpty())
            this.sourceWhere = sourceWhere;
    }

    public String getIncrementalWatermarkColumn() {
        return incrementalWatermarkColumn;
    }

    public void setIncrementalWatermarkColumn(String incrementalWatermarkColumn) {
        this.incrementalWatermarkColumn = incrementalWatermarkColumn;
    }

    public void setIncrementalWatermarkColumnNotNull(String incrementalWatermarkColumn) {
        if (incrementalWatermarkColumn != null && !incrementalWatermarkColumn.isEmpty())
            this.incrementalWatermarkColumn = incrementalWatermarkColumn;
    }

    public String getIncrementalWatermarkValue() {
        return incrementalWatermarkValue;
    }

    public void setIncrementalWatermarkValue(String incrementalWatermarkValue) {
        this.incrementalWatermarkValue = incrementalWatermarkValue;
    }

    public void setIncrementalWatermarkValueNotNull(String incrementalWatermarkValue) {
        if (incrementalWatermarkValue != null && !incrementalWatermarkValue.isEmpty())
            this.incrementalWatermarkValue = incrementalWatermarkValue;
    }

    public String getSourceQuery() {
        return sourceQuery;
    }

    public void setSourceQuery(String sourceQuery) {
        this.sourceQuery = sourceQuery;
    }

    public void setSourceQueryNotNull(String sourceQuery) {
        if (sourceQuery != null && !sourceQuery.isEmpty())
            this.sourceQuery = sourceQuery;
    }

    public String getSinkConnect() {
        return sinkConnect;
    }

    public void setSinkConnect(String sinkConnect) {
        this.sinkConnect = sinkConnect;
    }

    public void setSinkConnectNotNull(String sinkConnect) {
        if (sinkConnect != null && !sinkConnect.isEmpty())
            this.sinkConnect = sinkConnect;
    }

    public String getSinkUser() {
        return sinkUser;
    }

    public void setSinkUser(String sinkUser) {
        this.sinkUser = sinkUser;
    }

    public void setSinkUserNotNull(String sinkUser) {
        if (sinkUser != null && !sinkUser.isEmpty())
            this.sinkUser = sinkUser;
    }

    public void setSinkAuthMode(String mode) {
        sinkAuthentication.setMode(mode);
    }

    public void setSinkAuthModeNotNull(String mode) {
        if (mode != null && !mode.isEmpty())
            setSinkAuthMode(mode);
    }

    public void setSinkAuthPrincipalId(String principalId) {
        sinkAuthentication.setPrincipalId(principalId);
    }

    public void setSinkAuthPrincipalIdNotNull(String principalId) {
        if (principalId != null && !principalId.isEmpty())
            setSinkAuthPrincipalId(principalId);
    }

    public void setSinkAuthLoginHint(String loginHint) {
        sinkAuthentication.setLoginHint(loginHint);
    }

    public void setSinkAuthLoginHintNotNull(String loginHint) {
        if (loginHint != null && !loginHint.isEmpty())
            setSinkAuthLoginHint(loginHint);
    }

    public void setSinkAuthClientCertificate(String clientCertificate) {
        sinkAuthentication.setClientCertificate(clientCertificate);
    }

    public void setSinkAuthClientCertificateNotNull(String clientCertificate) {
        if (clientCertificate != null && !clientCertificate.isEmpty())
            setSinkAuthClientCertificate(clientCertificate);
    }

    public void setSinkAuthClientKey(String clientKey) {
        sinkAuthentication.setClientKey(clientKey);
    }

    public void setSinkAuthClientKeyNotNull(String clientKey) {
        if (clientKey != null && !clientKey.isEmpty())
            setSinkAuthClientKey(clientKey);
    }

    public String getSinkPassword() {
        return sinkPassword;
    }

    public void setSinkPassword(String sinkPassword) {
        this.sinkPassword = sinkPassword;
    }

    public void setSinkPasswordNotNull(String sinkPassword) {
        if (sinkPassword != null && !sinkPassword.isEmpty())
            this.sinkPassword = sinkPassword;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public void setSinkTableNotNull(String sinkTable) {
        if (sinkTable != null && !sinkTable.isEmpty())
            this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public void setSinkColumnsNotNull(String sinkColumns) {
        if (sinkColumns != null && !sinkColumns.isEmpty())
            this.sinkColumns = sinkColumns;
    }


    public Boolean getSinkDisableIndex() {
        return sinkDisableIndex;
    }

    public void setSinkDisableIndex(Boolean sinkDisableIndex) {
        this.sinkDisableIndex = sinkDisableIndex;
    }

    public void setSinkDisableIndexNotNull(Boolean sinkDisableIndex) {
        if (sinkDisableIndex != null)
            this.sinkDisableIndex = sinkDisableIndex;
    }


    public int getJobs() {
        return jobs;
    }

    public void setJobs(String jobs) {
        try {
            if (jobs != null && !jobs.isEmpty()) {
                this.jobs = Integer.parseInt(jobs);
                if (this.jobs <= 0) throw new NumberFormatException();
            }
        } catch (NumberFormatException | NullPointerException e) {
            LOG.error("Option --jobs must be a positive integer grater than 0.");
            throw e;
        }
    }

    public void setJobsNotNull(String jobs) {
        if (jobs != null && !jobs.isEmpty())
            setJobs(jobs);
    }

    public Boolean isHelp() {
        return help;
    }

    public void setHelp(Boolean help) {
        this.help = help;
    }


    public Level getVerboseLevel() {
        return verboseLevel;
    }

    public void setVerboseLevel(Level verboseLevel) {
        this.verboseLevel = verboseLevel;
    }

    public String getOptionsFile() {
        return optionsFile;
    }

    public void setOptionsFile(String optionsFile) {
        this.optionsFile = optionsFile;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {

        if (mode != null && !mode.isEmpty()) {
            if (!mode.toLowerCase().equals(ReplicationMode.COMPLETE.getModeText())
                    && !mode.toLowerCase().equals(ReplicationMode.INCREMENTAL.getModeText())
                    && !mode.toLowerCase().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())
            )
                throw new IllegalArgumentException("mode option must be "
                        + ReplicationMode.COMPLETE.getModeText()
                        + ", "
                        + ReplicationMode.COMPLETE_ATOMIC.getModeText()
                        + " or "
                        + ReplicationMode.INCREMENTAL.getModeText()
                        + ". CDC mode is no longer supported."
                );
        } else {
            // Default mode
            mode = ReplicationMode.COMPLETE.getModeText();
        }
        this.mode = mode.toLowerCase();
    }

    public void setModeNotNull(String mode) {
        if (mode != null && !mode.isEmpty())
            setMode(mode);
    }

    public Boolean isSinkDisableEscape() {
        return sinkDisableEscape;
    }

    public void setSinkDisableEscape(Boolean sinkDisableEscape) {
        this.sinkDisableEscape = sinkDisableEscape;
    }

    public void setSinkDisableEscapeNotNull(Boolean sinkDisableEscape) {
        if (sinkDisableEscape != null)
            this.sinkDisableEscape = sinkDisableEscape;
    }

    public Boolean isSinkDisableTruncate() {
        return sinkDisableTruncate;
    }

    public void setSinkDisableTruncate(Boolean sinkDisableTruncate) {
        this.sinkDisableTruncate = sinkDisableTruncate;
    }

    private void setSinkDisableTruncateNotNull(Boolean sinkDisableTruncate) {
        if (sinkDisableTruncate != null)
            this.sinkDisableTruncate = sinkDisableTruncate;
    }

    public Boolean isSinkAutoCreate() {
        return sinkAutoCreate;
    }

    public void setSinkAutoCreate(Boolean sinkAutoCreate) {
        this.sinkAutoCreate = sinkAutoCreate;
    }

    private void setSinkAutoCreateNotNull(Boolean sinkAutoCreate) {
        if (sinkAutoCreate != null)
            this.sinkAutoCreate = sinkAutoCreate;
    }


    public Boolean getSinkAnalyze() {
        return sinkAnalyze;
    }

    public void setSinkAnalyze(Boolean sinkAnalyze) {
        this.sinkAnalyze = sinkAnalyze;
    }

    public void setSinkAnalyzeNotNull(Boolean sinkAnalyze) {
        if (sinkAnalyze != null)
            this.sinkAnalyze = sinkAnalyze;
    }

    public Properties getSourceConnectionParams() {
        return sourceConnectionParams;
    }

    public void setSourceConnectionParams(Properties sourceConnectionParams) {
        this.sourceConnectionParams = sourceConnectionParams;
    }

    public Properties getSinkConnectionParams() {
        return sinkConnectionParams;
    }

    public void setSinkConnectionParams(Properties sinkConnectionParams) {
        this.sinkConnectionParams = sinkConnectionParams;
    }

    public AzureAuthenticationOptions getSourceAuthentication() {
        return sourceAuthentication;
    }

    public void setSourceAuthentication(AzureAuthenticationOptions sourceAuthentication) {
        this.sourceAuthentication = sourceAuthentication == null
                ? new AzureAuthenticationOptions()
                : sourceAuthentication;
    }

    public AzureAuthenticationOptions getSinkAuthentication() {
        return sinkAuthentication;
    }

    public void setSinkAuthentication(AzureAuthenticationOptions sinkAuthentication) {
        this.sinkAuthentication = sinkAuthentication == null
                ? new AzureAuthenticationOptions()
                : sinkAuthentication;
    }

    public void validateAzureAuthentication() {
        sourceAuthentication.validate(hasValue(sourceUser), hasValue(sourcePassword));
        sinkAuthentication.validate(hasValue(sinkUser), hasValue(sinkPassword));

        if (jobs > 1 && (isInteractive(sourceAuthentication) || isInteractive(sinkAuthentication))) {
            throw new IllegalArgumentException(
                    "ActiveDirectoryInteractive authentication requires jobs=1 to avoid concurrent browser flows.");
        }
    }

    private boolean isInteractive(AzureAuthenticationOptions authentication) {
        return authentication != null
                && AzureAuthenticationMode.ACTIVE_DIRECTORY_INTERACTIVE.equals(authentication.getMode());
    }

    private boolean hasValue(String value) {
        return value != null && !value.isBlank();
    }


    public String getSinkStagingTable() {
        return sinkStagingTable;
    }

    public void setSinkStagingTable(String sinkStagingTable) {
        this.sinkStagingTable = sinkStagingTable;
    }

    public void setSinkStagingTableNotNull(String sinkStagingTable) {
        if (sinkStagingTable != null)
            this.sinkStagingTable = sinkStagingTable;
    }


    public String getSinkStagingTableAlias() {
        return sinkStagingTableAlias;
    }

    public void setSinkStagingTableAlias(String sinkStagingTableAlias) {
        this.sinkStagingTableAlias = sinkStagingTableAlias;
    }

    public void setSinkStagingTableAliasNotNull(String sinkStagingTableAlias) {
        if (sinkStagingTableAlias != null)
            this.sinkStagingTableAlias = sinkStagingTableAlias;
    }

    public String getSinkStagingSchema() {
        return sinkStagingSchema;
    }

    public void setSinkStagingSchema(String sinkStagingSchema) {
        this.sinkStagingSchema = sinkStagingSchema;
    }

    public void setSinkStagingSchemaNotNull(String sinkStagingSchema) {
        if (sinkStagingSchema != null)
            this.sinkStagingSchema = sinkStagingSchema;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSizeNotNull(String fetchSize) {
        if (fetchSize != null && !fetchSize.isEmpty())
            setFetchSize(fetchSize);
    }

    public void setFetchSize(String fetchSize) {
        try {
            if (fetchSize != null && !fetchSize.isEmpty()) {
                this.fetchSize = Integer.parseInt(fetchSize);
                if (this.fetchSize <= 0) throw new NumberFormatException();
            }
        } catch (NumberFormatException | NullPointerException e) {
            LOG.error("Option --fetch-size must be a positive integer grater than 0.");
            throw e;
        }

    }

    @Override
    public String toString() {
        return "ToolOptions{" +
            " \n\tsourceConnect='" + CredentialRedactor.redactConnectionString(sourceConnect) + '\'' +
            ",\n\tsourceUser='" + CredentialRedactor.redactIdentity(sourceUser) + '\'' +
                ",\n\tsourcePassword='" + (sourcePassword != null ? "****" : "null") + '\'' +
                ",\n\tsourceTable='" + sourceTable + '\'' +
                ",\n\tsourceColumns='" + sourceColumns + '\'' +
                ",\n\tsourceWhere='" + sourceWhere + '\'' +
                ",\n\tsourceQuery='" + sourceQuery + '\'' +
                ",\n\tsinkConnect='" + CredentialRedactor.redactConnectionString(sinkConnect) + '\'' +
                ",\n\tsinkUser='" + CredentialRedactor.redactIdentity(sinkUser) + '\'' +
                ",\n\tsinkPassword='" + (sinkPassword != null ? "****" : "null") + '\'' +
                ",\n\tsinkTable='" + sinkTable + '\'' +
                ",\n\tsinkStagingTable='" + sinkStagingTable + '\'' +
                ",\n\tsinkStagingSchema='" + sinkStagingSchema + '\'' +
                ",\n\tsinkStagingTableAlias='" + sinkStagingTableAlias + '\'' +
                ",\n\tsinkColumns='" + sinkColumns + '\'' +
                ",\n\tsinkDisableEscape=" + sinkDisableEscape +
                ",\n\tsinkDisableIndex=" + sinkDisableIndex +
                ",\n\tsinkDisableTruncate=" + sinkDisableTruncate +
                ",\n\tsinkAutoCreate=" + sinkAutoCreate +
                ",\n\tsinkAnalyze=" + sinkAnalyze +
                ",\n\tjobs=" + jobs +
                ",\n\tBandwidthThrottling=" + bandwidthThrottling +
                ",\n\tquotedIdentifiers=" + quotedIdentifiers +
                ",\n\tfetchSize=" + fetchSize +
                ",\n\thelp=" + help +
                ",\n\tversion=" + version +
                ",\n\tverbose=" + verboseLevel +
                ",\n\toptionsFile='" + optionsFile + '\'' +
                ",\n\tmode='" + mode + '\'' +
                ",\n\tsentryDsn='" + CredentialRedactor.redactIdentity(sentryDsn) + '\'' +
                ",\n\tsourceConnectionParams=" + CredentialRedactor.redactProperties(sourceConnectionParams) +
                ",\n\tsinkConnectionParams=" + CredentialRedactor.redactProperties(sinkConnectionParams) +
                ",\n\tsourceAuthentication=" + sourceAuthentication +
                ",\n\tsinkAuthentication=" + sinkAuthentication +
                ",\n\tsourceFileFormat='" + sourceFileFormat + '\'' +
                ",\n\tsinkFileformat='" + sinkFileFormat + '\'' +
                '}';
    }


    public int getBandwidthThrottling() {
        return bandwidthThrottling;
    }

    public void setBandwidthThrottling(String bandwidthThrottling) {
        try {
            if (bandwidthThrottling != null && !bandwidthThrottling.isEmpty()) {
                this.bandwidthThrottling = Integer.parseInt(bandwidthThrottling);
                if (this.bandwidthThrottling < 0) throw new NumberFormatException();
            }
        } catch (NumberFormatException | NullPointerException e) {
            LOG.error("Option --bandwidth-throttling must be a positive integer grater than 0.");
            throw e;
        }
    }

    public void setBandwidthThrottlingNotNull(String bandwidthThrottling) {
        if (bandwidthThrottling != null && !bandwidthThrottling.isEmpty())
            setBandwidthThrottling(bandwidthThrottling);
    }

    public Boolean getQuotedIdentifiers() {
        return quotedIdentifiers;
    }

    public void setQuotedIdentifiers(Boolean quotedIdentifiers) {
        this.quotedIdentifiers = quotedIdentifiers;
    }

    public String getSourceFileFormat() {
        return sourceFileFormat;
    }

    public void setSourceFileFormat(String sourceFileFormat) {
        this.sourceFileFormat = sourceFileFormat;
    }

    private void setSourceFileFormatNotNull(String fileFormat) {
        if (fileFormat != null && !fileFormat.isEmpty())
            this.sourceFileFormat = fileFormat;
    }

    public String getSinkFileFormat() {
        return sinkFileFormat;
    }

    public void setSinkFileFormat(String sinkFileFormat) {
        this.sinkFileFormat = sinkFileFormat;
    }

    private void setSinkFileFormatNotNull(String fileFormat) {
        if (fileFormat != null && !fileFormat.isEmpty())
            this.sinkFileFormat = fileFormat;
    }

    public String getSentryDsn() {
        return sentryDsn;
    }

    public void setSentryDsn(String sentryDsn) {
        this.sentryDsn = sentryDsn;
    }

    public List<ColumnDescriptor> getSourceColumnDescriptors() {
        return sourceColumnDescriptors;
    }

    public void setSourceColumnDescriptors(List<ColumnDescriptor> sourceColumnDescriptors) {
        this.sourceColumnDescriptors = sourceColumnDescriptors;
    }

    public String[] getSourcePrimaryKeys() {
        return sourcePrimaryKeys;
    }

    public void setSourcePrimaryKeys(String[] sourcePrimaryKeys) {
        this.sourcePrimaryKeys = sourcePrimaryKeys;
    }

    public List<ReplicationTable> getReplicationTables() {
        return replicationTables;
    }

    public boolean hasReplicationTables() {
        return !replicationTables.isEmpty();
    }

    private void setReplicationTables(List<ReplicationTable> replicationTables) {
        this.replicationTables = replicationTables == null ? List.of() : List.copyOf(replicationTables);
    }

}
