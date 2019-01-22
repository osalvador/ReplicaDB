package org.replicadb.manager;

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import java.util.Random;

public class CsvManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(CsvManager.class.getName());

    // String array with the paths of the temporal files
    private static String[] tempFilesPath;

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

        CsvWriter csvWriter = new CsvWriter();
        // Custom user csv options
        setCsvWriterOptions(csvWriter);

        // Write the CSV
        try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {

            // headers, only in the first temporal file.
            if (taskId == 0 && Boolean.valueOf(options.getSinkConnectionParams().getProperty("Header"))) {
                for (int i = 1; i <= columnsNumber; i++) {
                    csvAppender.appendField(rsmd.getColumnName(i));
                }
                csvAppender.endLine();
            }

            String colValue;
            String[] colValues;

            // lines
            while (resultSet.next()) {
                colValues = new String[columnsNumber];

                // Iterate over the columns of the row
                for (int i = 1; i <= columnsNumber; i++) {
                    colValue = resultSet.getString(i);

                    if (!this.options.isSinkDisableEscape() && !resultSet.wasNull())
                        colValues[i - 1] = colValue.replace("\n", "\\n").replace("\r", "\\r");
                    else
                        colValues[i - 1] = colValue;
                }
                csvAppender.appendLine(colValues);
            }
        }

        return 0;
    }

    /**
     * Retrieves and sets the custom options that define the CSV Writer
     *
     * @param writer
     */
    private void setCsvWriterOptions(CsvWriter writer) {
        Properties fileProperties = options.getSinkConnectionParams();

        // Header option is not supported on incremental mode
        if (Boolean.valueOf(options.getSinkConnectionParams().getProperty("Header"))
                && options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            throw new IllegalArgumentException("Header option is not supported on incremental mode");
        }

        String fieldSeparator, textDelimiter, lineDelimiter;

        fieldSeparator = fileProperties.getProperty("FieldSeparator");
        textDelimiter = fileProperties.getProperty("TextDelimiter");
        lineDelimiter = fileProperties.getProperty("LineDelimiter");
        boolean alwaysDelimitText = Boolean.valueOf(fileProperties.getProperty("AlwaysDelimitText"));

        if (fieldSeparator != null && fieldSeparator.length() > 1)
            throw new IllegalArgumentException("FieldSeparator must be a single char");

        if (textDelimiter != null && textDelimiter.length() > 1)
            throw new IllegalArgumentException("TextDelimiter must be a single char");

        char charFieldSeparator, charTextDelimiter;

        if (fieldSeparator != null && !fieldSeparator.isEmpty()) {
            charFieldSeparator = fieldSeparator.charAt(0);
            writer.setFieldSeparator(charFieldSeparator);
        }
        if (textDelimiter != null && !textDelimiter.isEmpty()) {
            charTextDelimiter = textDelimiter.charAt(0);
            writer.setTextDelimiter(charTextDelimiter);
        }
        if (lineDelimiter != null && !lineDelimiter.isEmpty()) {
            writer.setLineDelimiter(lineDelimiter.toCharArray());
        }

        writer.setAlwaysDelimitText(alwaysDelimitText);
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
    public void preSourceTasks() {/*Not implemented*/}

    @Override
    public void postSourceTasks() {/*Not implemented*/}


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
