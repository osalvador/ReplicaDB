package org.replicadb.manager;

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class CsvManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(CsvManager.class.getName());

    private static String[] tempFilesPath;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public CsvManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        // Fixed size
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
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException, URISyntaxException {

        try {

            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            String randomFileUrl = options.getSinkConnect() + ".repdb." + (new Random().nextInt(1000) + 9000);
            LOG.debug("CSV path: " + randomFileUrl);
            tempFilesPath[taskId] = randomFileUrl;

            File file = getFileFromPathString(randomFileUrl);

            CsvWriter csvWriter = new CsvWriter();
            setCsvWriterOptions(csvWriter);

            try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {

                // headers, only in the first temporal file.
                if (taskId == 0 && Boolean.valueOf(options.getSinkConnectionParams().getProperty("Header"))) {
                    for (int i = 1; i <= columnsNumber; i++) {
                        csvAppender.appendField(rsmd.getColumnName(i));
                    }
                    csvAppender.endLine();
                }

                String colValue;
                String[] colValues; //= new String[columnsNumber];

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

        } catch (Exception e) {
            throw e;
        }

        return 0;
    }

    private void setCsvWriterOptions(CsvWriter writer) {
        Properties fileProperties = options.getSinkConnectionParams();
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
    protected void createStagingTable() throws SQLException {

    }

    @Override
    protected void mergeStagingTable() {

        try {

//
//        URL finalFile = new URL(options.getSinkConnect());
//        LOG.debug("Final File PATH: " + finalFile.getPath());
//        LOG.debug("Final File FILE: " + finalFile.getFile());
//
//
//
//        LOG.debug("Final File URI: " + finalFile.toURI());


            File finalFile = getFileFromPathString(options.getSinkConnect());
            File firstTemporalFile = getFileFromPathString(tempFilesPath[0]);
            Path firstTemporalFilePath = Paths.get(firstTemporalFile.getPath());

            // Rename first temporal file to the final file
            Files.move(firstTemporalFilePath, firstTemporalFilePath.resolveSibling(finalFile.getPath()), StandardCopyOption.REPLACE_EXISTING);


            // Channel for append to the final file
            FileChannel finalFileChannel = new FileOutputStream(finalFile, true).getChannel();

            for (int i = 1; i <= tempFilesPath.length - 1; i++) {
                FileChannel tempFileChannel = new FileInputStream(getFileFromPathString(tempFilesPath[i])).getChannel();
                finalFileChannel.transferFrom(tempFileChannel, finalFileChannel.size(), tempFileChannel.size());
                tempFileChannel.close();
                boolean isDeleted = getFileFromPathString(tempFilesPath[i]).delete();
                LOG.debug("isDeleted:"+isDeleted);
            }

            finalFileChannel.close();

        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void preSourceTasks() {/*Not implemented*/}

    @Override
    public void postSourceTasks() {/*Not implemented*/}


    @Override
    public void postSinkTasks() throws IOException {
        LOG.debug(Arrays.toString(tempFilesPath));
        // Always merge data
        this.mergeStagingTable();
    }

    @Override
    public void cleanUp() throws Exception {
        // Ensure drop temporal file
        for (int i = 0; i <= tempFilesPath.length - 1; i++){
            boolean isDeleted = getFileFromPathString(tempFilesPath[i]).delete();
            LOG.debug("isDeleted:"+isDeleted);
        }
    }


    private File getFileFromPathString(String urlString) throws MalformedURLException, URISyntaxException {

        URL url = new URL(urlString);

        URI uri = url.toURI();

        if (uri.getAuthority() != null && uri.getAuthority().length() > 0) {
            // Hack for UNC Path
            uri = (new URL("file://" + urlString.substring("file:".length()))).toURI();
        }

        File file = new File(uri);
        LOG.debug("File is: " + file.toString());

        return file;
    }

}
