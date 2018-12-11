package org.replicadb.cli;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public class ToolOptions {


    private static final Logger LOG = LogManager.getLogger(ToolOptions.class.getName());
    private static final int DEFAULT_JOBS = 4;
    private static final String DEFAULT_MODE = ReplicationMode.COMPLETE.getModeText();

    private String sourceConnect;
    private String sourceUser;
    private String sourcePassword;
    private String sourceTable;
    private String sourceColumns;
    private String sourceWhere;
    private String sourceQuery;
    private String sourceCheckColumn; // for incremental mode
    private String sourceLastValue; // for incremental mode

    private String sinkConnect;
    private String sinkUser;
    private String sinkPassword;
    private String sinkTable;
    private String sinkColumns;
    private Boolean sinkDisableEscape = false;
    private Boolean sinkDisableIndex = false;
    private Boolean sinkDisableTruncate = false;
    private Boolean sinkAnalyze = false;


    private int jobs =DEFAULT_JOBS;
    private Boolean help = false;
    private Boolean verbose = false;
    private String optionsFile;

    private String mode = DEFAULT_MODE;

    private Options options;

    public ToolOptions(String[] args) throws ParseException, IOException {
        checkOptions(args);
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
                        .longOpt("source-query")
                        .desc("SQL statement to be executed in the source database")
                        .hasArg()
                        .argName("statement")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("source-check-column")
                        .desc("Specify the column to be examined when determining which rows to be replicated")
                        .hasArg()
                        .argName("column")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-last-value")
                        .desc("Specifies the maximum value of the source-check-column from the previous replication")
                        .hasArg()
                        .argName("value")
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
                        .longOpt("sink-analyze")
                        .desc("Analyze sink database table after populate.")
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
                        .desc("Specifies the replication mode. The allowed values are complete or incremental")
                        //.required()
                        .hasArg()
                        .argName("mode")
                        .build()
        );


        Option helpOpt = new Option("h", "help", false, "Print this help screen");
        options.addOption(helpOpt);

        Option jobsOpt = new Option("j", "jobs", true, "Use n jobs to replicate in parallel. Default 4");
        jobsOpt.setArgName("n");
        options.addOption(jobsOpt);

        Option verboseOpt = new Option("v", "verbose", false, "Print more information while working");
        options.addOption(verboseOpt);


        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // If help argument is not passed is not necessary test the rest of arguments
        if (existsHelpArgument(args)) {
            printHelp();
            this.setHelp(true);
        } else {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            // check for optionsFile
            setOptionsFile(line.getOptionValue("options-file"));
            if (this.optionsFile != null && !this.optionsFile.isEmpty()) {
                loadOptionsFile();
            }

            //get & set Options
            if (line.hasOption("verbose")) setVerbose(true);
            if (line.hasOption("sink-disable-index")) setSinkDisableIndexNotNull(true);
            if (line.hasOption("sink-disable-escape")) setSinkDisableEscapeNotNull(true);
            if (line.hasOption("sink-disable-truncate")) setSinkDisableTruncateNotNull(true);
            if (line.hasOption("sink-analyze")) setSinkAnalyzeNotNull(true);

            setModeNotNull(line.getOptionValue("mode"));
            setSinkColumnsNotNull(line.getOptionValue("sink-columns"));
            setSinkConnectNotNull(line.getOptionValue("sink-connect"));
            setHelp(line.hasOption("help"));
            setSinkPasswordNotNull(line.getOptionValue("sink-password"));
            setSinkTableNotNull(line.getOptionValue("sink-table"));
            setSinkUserNotNull(line.getOptionValue("sink-user"));
            setSourceCheckColumnNotNull(line.getOptionValue("source-check-column"));
            setSourceColumnsNotNull(line.getOptionValue("source-columns"));
            setSourceConnectNotNull(line.getOptionValue("source-connect"));
            setSourceLastValueNotNull(line.getOptionValue("source-last-value"));
            setSourcePasswordNotNull(line.getOptionValue("source-password"));
            setSourceQueryNotNull(line.getOptionValue("source-query"));
            setSourceTableNotNull(line.getOptionValue("source-table"));
            setSourceUserNotNull(line.getOptionValue("source-user"));
            setSourceWhereNotNull(line.getOptionValue("source-where"));
            setJobsNotNull(line.getOptionValue("jobs"));


            //Check for required values
            if (!checkRequiredValues()) throw new IllegalArgumentException("Missing any of the required parameters:" +
                    " source-connect=" + this.sourceConnect + " OR sink-connect=" + this.sinkConnect);
        }

    }


    private void printHelp() {
        String header = "\nArguments: \n";
        String footer = "\nPlease report issues at https://github.com/osalvador/ReplicaDB/issues";

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
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

        setVerbose(Boolean.parseBoolean(prop.getProperty("verbose")));
        setMode(prop.getProperty("mode"));

        setSinkColumns(prop.getProperty("sink.columns"));
        setSinkConnect(prop.getProperty("sink.connect"));
        setSinkDisableIndex(Boolean.parseBoolean(prop.getProperty("sink.disable.index")));
        setSinkDisableEscape(Boolean.parseBoolean(prop.getProperty("sink.disable.escape")));
        setSinkDisableTruncate(Boolean.parseBoolean(prop.getProperty("sink.disable.truncate")));
        setSinkPassword(prop.getProperty("sink.password"));
        setSinkTable(prop.getProperty("sink.table"));
        setSinkUser(prop.getProperty("sink.user"));
        setSourceCheckColumn(prop.getProperty("source.check-column"));
        setSourceLastValue(prop.getProperty("source.last-value"));
        setSourceColumns(prop.getProperty("source.columns"));
        setSourceConnect(prop.getProperty("source.connect"));
        setSourcePassword(prop.getProperty("source.password"));
        setSourceQuery(prop.getProperty("source.query"));
        setSourceTable(prop.getProperty("source.table"));
        setSourceUser(prop.getProperty("source.user"));
        setSourceWhere(prop.getProperty("source.where"));
        setJobs(prop.getProperty("jobs"));

    }


    /*
     * Geeters & Setters
     */
    public String getSourceConnect() {
        return sourceConnect;
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

    public String getSourceCheckColumn() {
        return sourceCheckColumn;
    }

    public void setSourceCheckColumn(String sourceCheckColumn) {
        this.sourceCheckColumn = sourceCheckColumn;
    }

    public void setSourceCheckColumnNotNull(String sourceCheckColumn) {
        if (sourceCheckColumn != null && !sourceCheckColumn.isEmpty())
            this.sourceCheckColumn = sourceCheckColumn;
    }

    public String getSourceLastValue() {
        return sourceLastValue;
    }

    public void setSourceLastValue(String sourceLastValue) {
        this.sourceLastValue = sourceLastValue;
    }

    public void setSourceLastValueNotNull(String sourceLastValue) {
        if (sourceLastValue != null && !sourceLastValue.isEmpty())
            this.sourceLastValue = sourceLastValue;
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


    public Boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(Boolean verbose) {
        this.verbose = verbose;
    }

    public void setVerboseNotNull(Boolean verbose) {
        if (verbose != null)
            this.verbose = verbose;
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
            if (!mode.equals(ReplicationMode.COMPLETE.getModeText()) && !mode.equals(ReplicationMode.INCREMENTAL.getModeText()))
                throw new IllegalArgumentException("mode option must be " + ReplicationMode.COMPLETE.getModeText() + " or " + ReplicationMode.INCREMENTAL.getModeText());
        }

        this.mode = mode;
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

    @Override
    public String toString() {
        return "ToolOptions{" +
                " \n\tsourceConnect='" + sourceConnect + '\'' +
                ",\n\tsourceUser='" + sourceUser + '\'' +
                ",\n\tsourcePassword='" + sourcePassword + '\'' +
                ",\n\tsourceTable='" + sourceTable + '\'' +
                ",\n\tsourceColumns='" + sourceColumns + '\'' +
                ",\n\tsourceWhere='" + sourceWhere + '\'' +
                ",\n\tsourceQuery='" + sourceQuery + '\'' +
                ",\n\tsourceCheckColumn='" + sourceCheckColumn + '\'' +
                ",\n\tsourceLastValue='" + sourceLastValue + '\'' +
                ",\n\tsinkConnect='" + sinkConnect + '\'' +
                ",\n\tsinkUser='" + sinkUser + '\'' +
                ",\n\tsinkPassword='" + sinkPassword + '\'' +
                ",\n\tsinkTable='" + sinkTable + '\'' +
                ",\n\tsinkColumns='" + sinkColumns + '\'' +
                ",\n\tsinkDisableEscape=" + sinkDisableEscape +
                ",\n\tsinkDisableIndex=" + sinkDisableIndex +
                ",\n\tsinkAnalyze=" + sinkAnalyze +
                ",\n\tjobs=" + jobs +
                ",\n\thelp=" + help +
                ",\n\tverbose=" + verbose +
                ",\n\toptionsFile='" + optionsFile + '\'' +
                ",\n\tmode='" + mode + '\'' +
                '}';
    }
}
