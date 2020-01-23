package org.replicadb.manager;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.rowset.CsvCachedRowSetImpl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

public class CsvManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(CsvManager.class.getName());

    // String array with the paths of the temporal files
    private static String[] tempFilesPath;

    private CsvCachedRowSetImpl csvResultset;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public CsvManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        // Fixed size String Array
        tempFilesPath = new String[options.getJobs()];
    }

    @Override
    protected Connection makeSinkConnection() {
        /*Not necessary for csv*/
        return null;
    }

    @Override
    protected Connection makeSourceConnection() throws SQLException {
        // Initialize the csvResultSet
        try {
            csvResultset = new CsvCachedRowSetImpl();

            CSVFormat format = setCsvFormat(DataSourceType.SOURCE);
            csvResultset.setCsvFormat(format);

            csvResultset.setSourceFile(getFileFromPathString(options.getSourceConnect()));
            csvResultset.setFetchSize(options.getFetchSize());

            String columnsTypes = options.getSourceConnectionParams().getProperty("columns.types");
            if (columnsTypes == null || columnsTypes.isEmpty())
                throw new IllegalArgumentException("Parameter 'source.connect.parameter.columns.types' cannot be null");

            csvResultset.setColumnsTypes(columnsTypes);

            csvResultset.execute();

        } catch (URISyntaxException | MalformedURLException e) {
            throw new SQLException(e);
        }
        return null;
    }

    private CSVFormat setCsvFormat(DataSourceType dsType) {

        Properties formatProperties;
        CSVFormat csvFormat = CSVFormat.DEFAULT;

        if (dsType == DataSourceType.SOURCE) {
            formatProperties = options.getSourceConnectionParams();
        } else {
            formatProperties = options.getSinkConnectionParams();
        }

        // Predfined CSV Formats
        String predefinedFormats = formatProperties.getProperty("format");
        if (predefinedFormats != null && !predefinedFormats.isEmpty()) {
            switch (predefinedFormats.toUpperCase().trim()) {
                case "EXCEL":
                    csvFormat = CSVFormat.EXCEL;
                    LOG.debug("Setting the initial format to EXCEL");
                    break;
                case "INFORMIX_UNLOAD":
                    csvFormat = CSVFormat.INFORMIX_UNLOAD;
                    LOG.debug("Setting the initial format to INFORMIX_UNLOAD");
                    break;
                case "INFORMIX_UNLOAD_CSV":
                    csvFormat = CSVFormat.INFORMIX_UNLOAD_CSV;
                    LOG.debug("Setting the initial format to INFORMIX_UNLOAD_CSV");
                    break;
                case "MONGODB_CSV":
                    csvFormat = CSVFormat.MONGODB_CSV;
                    LOG.debug("Setting the initial format to MONGODB_CSV");
                    break;
                case "MONGODB_TSV":
                    csvFormat = CSVFormat.MONGODB_TSV;
                    LOG.debug("Setting the initial format to MONGODB_TSV");
                    break;
                case "MYSQL":
                    csvFormat = CSVFormat.MYSQL;
                    LOG.debug("Setting the initial format to MYSQL");
                    break;
                case "ORACLE":
                    csvFormat = CSVFormat.ORACLE;
                    LOG.debug("Setting the initial format to ORACLE");
                    break;
                case "POSTGRESQL_CSV":
                    csvFormat = CSVFormat.POSTGRESQL_CSV;
                    LOG.debug("Setting the initial format to POSTGRESQL_CSV");
                    break;
                case "POSTGRESQL_TEXT":
                    csvFormat = CSVFormat.POSTGRESQL_TEXT;
                    LOG.debug("Setting the initial format to POSTGRESQL_TEXT");
                    break;
                case "RFC4180":
                    csvFormat = CSVFormat.RFC4180;
                    LOG.debug("Setting the initial format to RFC4180");
                    break;
                case "TDF":
                    csvFormat = CSVFormat.TDF;
                    LOG.debug("Setting the initial format to TDF");
                    break;
                default:
                    csvFormat = CSVFormat.DEFAULT;
                    LOG.debug("Setting the initial format to DEFAULT");
                    break;
            }
        }

        // quoteMode
        String quoteMode = formatProperties.getProperty("format.quoteMode");
        if (quoteMode != null && !quoteMode.isEmpty()) {
            switch (quoteMode.toUpperCase().trim()) {
                case "ALL":
                    csvFormat = csvFormat.withQuoteMode(QuoteMode.ALL);
                    LOG.debug("Setting QuoteMode to ALL");
                    break;
                case "ALL_NON_NULL":
                    csvFormat = csvFormat.withQuoteMode(QuoteMode.ALL_NON_NULL);
                    LOG.debug("Setting QuoteMode to ALL_NON_NULL");
                    break;
                case "MINIMAL":
                    csvFormat = csvFormat.withQuoteMode(QuoteMode.MINIMAL);
                    LOG.debug("Setting QuoteMode to MINIMAL");
                    break;
                case "NON_NUMERIC":
                    csvFormat = csvFormat.withQuoteMode(QuoteMode.NON_NUMERIC);
                    LOG.debug("Setting QuoteMode to NON_NUMERIC");
                    break;
                case "NONE":
                    csvFormat = csvFormat.withQuoteMode(QuoteMode.NONE);
                    LOG.debug("Setting QuoteMode to NONE");
                    break;
            }
        }

        // Delimiter
        String delimiter = formatProperties.getProperty("format.delimiter");
        if (delimiter != null) {
            if (delimiter.length() == 1) {
                csvFormat = csvFormat.withDelimiter(delimiter.charAt(0));
            } else {
                throw new IllegalArgumentException("delimiter must be a single char");
            }
        }

        // Escape
        String escape = formatProperties.getProperty("format.escape");
        if (escape != null) {
            if (escape.length() == 1) {
                csvFormat = csvFormat.withEscape(escape.charAt(0));
            } else {
                throw new IllegalArgumentException("escape must be a single char");
            }
        }

        // quote
        String quote = formatProperties.getProperty("format.quote");
        if (quote != null) {
            if (quote.length() == 1) {
                csvFormat = csvFormat.withQuote(quote.charAt(0));
            } else {
                throw new IllegalArgumentException("quote must be a single char");
            }
        }

        // recordSeparator
        String recordSeparator = formatProperties.getProperty("format.recordSeparator");
        if (recordSeparator != null && !recordSeparator.isEmpty()) {
            csvFormat = csvFormat.withRecordSeparator(recordSeparator);
        }

        // nullString
        String nullString = formatProperties.getProperty("format.nullString");
        if (nullString != null && !nullString.isEmpty()) {
            csvFormat = csvFormat.withNullString(nullString);
        }

        // skipHeaderRecord
        String firstRecordAsHeader = formatProperties.getProperty("format.firstRecordAsHeader");
        if (firstRecordAsHeader != null && !firstRecordAsHeader.isEmpty()) {
            if (Boolean.parseBoolean(firstRecordAsHeader))
                csvFormat = csvFormat.withFirstRecordAsHeader();
        }

        // ignoreEmptyLines
        String ignoreEmptyLines = formatProperties.getProperty("format.ignoreEmptyLines");
        if (ignoreEmptyLines != null && !ignoreEmptyLines.isEmpty()) {
            csvFormat = csvFormat.withIgnoreEmptyLines(Boolean.parseBoolean(ignoreEmptyLines));
        }

        // ignoreSurroundingSpaces
        String ignoreSurroundingSpaces = formatProperties.getProperty("format.ignoreSurroundingSpaces");
        if (ignoreSurroundingSpaces != null && !ignoreSurroundingSpaces.isEmpty()) {
            csvFormat = csvFormat.withIgnoreSurroundingSpaces(Boolean.parseBoolean(ignoreSurroundingSpaces));
        }

        // trim
        String trim = formatProperties.getProperty("format.trim");
        if (trim != null && !trim.isEmpty()) {
            csvFormat = csvFormat.withTrim(Boolean.parseBoolean(trim));
        }

        LOG.debug("The final CSVFormat is: " + csvFormat);
        return csvFormat;

    }

    @Override
    protected void truncateTable() {
        /*Not necessary for csv*/
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.CSV.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws Exception {

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        // Temporal file name
        String randomFileUrl = options.getSinkConnect() + ".repdb." + (new Random().nextInt(1000) + 9000);
        LOG.info("Temp CSV file path: " + randomFileUrl);

        // Save the path of temp file
        tempFilesPath[taskId] = randomFileUrl;

        File file = getFileFromPathString(randomFileUrl);

        // Set csv format
        CSVFormat format = setCsvFormat(DataSourceType.SINK);

        // Write the CSV
        try (BufferedWriter out = Files.newBufferedWriter(file.toPath());
             CSVPrinter printer = new CSVPrinter(out, format)) {

            // headers, only in the first temporal file.
            if (taskId == 0 && format.getSkipHeaderRecord() ) {
                String[] allColumns = getAllSinkColumns(rsmd).split(",");
                for (int i = 0; i < columnsNumber; i++) {
                    printer.print(allColumns[i]);
                }
                printer.println();
            }
            printer.printRecords(resultSet);
        }

        return 0;
    }


    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {
        return this.csvResultset;
    }


    @Override
    protected void createStagingTable() {
    }

    @Override
    /**
     * Merging temporal files
     */
    protected void mergeStagingTable() throws Exception {

        File finalFile = getFileFromPathString(options.getSinkConnect());
        File firstTemporalFile = getFileFromPathString(tempFilesPath[0]);
        Path firstTemporalFilePath = Paths.get(firstTemporalFile.getPath());

        int tempFilesIdx = 0;
        if (!options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            // Rename first temporal file to the final file
            Files.move(firstTemporalFilePath, firstTemporalFilePath.resolveSibling(finalFile.getPath()), StandardCopyOption.REPLACE_EXISTING);
            tempFilesIdx = 1;
            LOG.info("Complete mode: creating and merging all temp files into: " + finalFile.getPath());
        } else {
            LOG.info("Incremental mode: appending and merging all temp files into: " + finalFile.getPath());
        }

        // Append the rest temporal files into final file
        try (FileChannel finalFileChannel = new FileOutputStream(finalFile, true).getChannel()) {

            // Starts with 1 because the first temp file was renamed.
            for (int i = tempFilesIdx; i <= tempFilesPath.length - 1; i++) {
                // Temp file channel
                FileChannel tempFileChannel = new FileInputStream(getFileFromPathString(tempFilesPath[i])).getChannel();
                // Append temp file to final file
                finalFileChannel.transferFrom(tempFileChannel, finalFileChannel.size(), tempFileChannel.size());
                tempFileChannel.close();
                // Delete temp file
                getFileFromPathString(tempFilesPath[i]).delete();
            }
        }

    }


    @Override
    public void postSourceTasks() {/*Not implemented*/}

    @Override
    public void preSourceTasks() {

        if (options.getJobs() > 1)
            throw new IllegalArgumentException("Only one job is allowed when reading from a file. Jobs parameter must be set to 1. jobs=1");

    }

    @Override
    public void postSinkTasks() throws Exception {
        // Always merge data
        this.mergeStagingTable();
    }

    @Override
    public void cleanUp() throws Exception {
        // Ensure drop temporal file
        for (int i = 0; i <= tempFilesPath.length - 1; i++) getFileFromPathString(tempFilesPath[i]).delete();
    }


    /**
     * Returns an instance of a File given the url string of the csv path.
     * It gives compatibility with the windows, linux and mac URL Strings.
     *
     * @param urlString
     * @return
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    private File getFileFromPathString(String urlString) throws MalformedURLException, URISyntaxException {

        URL url = new URL(urlString);
        URI uri = url.toURI();

        if (uri.getAuthority() != null && uri.getAuthority().length() > 0) {
            // Hack for UNC Path
            uri = (new URL("file://" + urlString.substring("file:".length()))).toURI();
        }

        File file = new File(uri);
        return file;
    }

}
