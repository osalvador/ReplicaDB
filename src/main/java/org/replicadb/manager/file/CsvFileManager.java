package org.replicadb.manager.file;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.util.BandwidthThrottling;
import org.replicadb.rowset.CsvCachedRowSetImpl;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

import static org.replicadb.manager.LocalFileManager.getFileFromPathString;
import static org.replicadb.manager.util.SqlNames.getAllSinkColumns;

public class CsvFileManager extends FileManager {
    private static final Logger LOG = LogManager.getLogger(CsvFileManager.class);

    public CsvFileManager(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
    }

    private CsvCachedRowSetImpl csvResultset;

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

        LOG.info("The final CSVFormat is: " + csvFormat);
        return csvFormat;

    }

    @Override
    public void init() throws SQLException {
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
            csvResultset.setColumnsNames(options.getSourceColumns());

            csvResultset.execute();

        } catch (URISyntaxException | MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet readData() {
        return this.csvResultset;
    }

    @Override
    public int writeData(OutputStream out, ResultSet resultSet, int taskId, File tempFile) throws IOException, SQLException {
        int totalRows = 0;
        // Set csv format
        CSVFormat format = setCsvFormat(DataSourceType.SINK);

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Write the CSV to the OutputStream
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(out));
             CSVPrinter printer = new CSVPrinter(bufferedWriter, format)) {

            // headers, only in the first temporal file.
            if (taskId == 0 && format.getSkipHeaderRecord()) {
                String[] allColumns = getAllSinkColumns(options, rsmd).split(",");
                for (int i = 0; i < columnCount; i++) {
                    printer.print(allColumns[i]);
                }
                printer.println();
            }

            if (resultSet.next()) {
                // Create Bandwidth Throttling
                BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);
                do {
                    bt.acquiere();
                    for (int i = 1; i <= columnCount; ++i) {
                        Object object = resultSet.getObject(i);
                        if (object instanceof Clob) {
                            printer.print(((Clob) object).getCharacterStream());
                        } else if (object instanceof SQLXML) {
                            printer.print(((SQLXML) object).getCharacterStream());
                        } else
                            printer.print(object);
                    }
                    printer.println();
                    totalRows++;
                } while (resultSet.next());
            }

        }

        return totalRows;
    }

    @Override
    public void mergeFiles() throws IOException, URISyntaxException {
        File finalFile = getFileFromPathString(options.getSinkConnect());
        File firstTemporalFile = getFileFromPathString(getTempFilePath(0));
        Path firstTemporalFilePath = Paths.get(firstTemporalFile.getPath());

        int tempFilesIdx = 0;
        if (!options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            // Rename first temporal file to the final file
            Files.move(firstTemporalFilePath, firstTemporalFilePath.resolveSibling(finalFile.getPath()), StandardCopyOption.REPLACE_EXISTING);
            tempFilesIdx = 1;
            LOG.info("Complete mode: creating and merging all temp files into: {}", finalFile.getPath());
        } else {
            LOG.info("Incremental mode: appending and merging all temp files into: {}", finalFile.getPath());
        }

        // Append the rest temporal files into final file
        try (FileChannel finalFileChannel = new FileOutputStream(finalFile, true).getChannel()) {

            // Starts with 1 because the first temp file was renamed.
            for (int i = tempFilesIdx; i <= getTempFilePathSize() - 1; i++) {
                // Temp file channel
                FileChannel tempFileChannel = new FileInputStream(getFileFromPathString(getTempFilePath(i))).getChannel();
                // Append temp file to final file
                finalFileChannel.transferFrom(tempFileChannel, finalFileChannel.size(), tempFileChannel.size());
                tempFileChannel.close();
                // Delete temp file
                getFileFromPathString(getTempFilePath(i)).delete();
            }
        }

    }

    @Override
    public void cleanUp() throws Exception {
        // Ensure drop all temporal files
        for (Map.Entry<Integer, String> filePath : getTempFilesPath().entrySet()) {
            File tempFile = getFileFromPathString(filePath.getValue());

            String crcPath = "file://" + tempFile.getParent() + "/." + tempFile.getName() + ".crc";
            File crcFile = getFileFromPathString(crcPath);

            if (tempFile.exists()) {
                LOG.debug("Remove temp file {}", tempFile.getPath());
                tempFile.delete();
            }
            if (crcFile.exists()) {
                LOG.debug("Remove crc temp file {}", crcFile.getPath());
                crcFile.delete();
            }
        }
    }
}
