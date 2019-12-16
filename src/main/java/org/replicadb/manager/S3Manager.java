package org.replicadb.manager;


import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.Properties;

public class S3Manager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(S3Manager.class.getName());

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public S3Manager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    protected Connection makeSinkConnection() {
        /*Not necessary for S3*/
        return null;
    }

    @Override
    protected void truncateTable() {
        /*Not necessary for S3*/
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.S3.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws Exception {

        Properties s3Props = options.getSinkConnectionParams();
        String accessKey = s3Props.getProperty("accessKey");
        String secretKey = s3Props.getProperty("secretKey");

        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        URI s3Uri = new URI(options.getSinkConnect());
        // Get Service Endpoint
        // Secure by default
        String serviceEndpoint = "https://";
        String encryptation = s3Props.getProperty("secure-connection");
        if (encryptation != null && !encryptation.isEmpty() && Boolean.parseBoolean(encryptation) == false) {
            serviceEndpoint = "http://";
        }

        // Get Service Port
        String servicePort = "";
        if (s3Uri.getPort() != -1) {
            servicePort = ":" + s3Uri.getPort();
        }

        serviceEndpoint = serviceEndpoint + s3Uri.getHost() + servicePort;
        LOG.debug("Using serviceEndpoint: " + serviceEndpoint);

        // Get Bucket name
        String bucketName = s3Uri.getPath().split("/")[1];
        LOG.debug("Using bucketName: " + bucketName);

        // S3 Client
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withRegion("")
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        // Get Replicadb custom properties
        LOG.debug("S3 properties: " + s3Props);
        Boolean rowIsObject = Boolean.parseBoolean(s3Props.getProperty("row.isObject"));

        if (!rowIsObject) {
            // All rows are only one CSV object in s3
            putCsvToS3(resultSet, taskId, s3Client, bucketName);
        } else {
            // Each row is a different object in s3
            putObjectToS3(resultSet, taskId, s3Client, bucketName);
        }


        return 0;

    }

    private void putObjectToS3(ResultSet resultSet, int taskId, AmazonS3 s3Client, String bucketName) throws SQLException, IOException {

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Get sinl properties
        Properties s3Props = options.getSinkConnectionParams();

        String rowKeyColumnName = s3Props.getProperty("row.keyColumn");
        String rowContentColumnName = s3Props.getProperty("row.contentColumn");

        // Get content column index
        int rowContentColumnIndex = 0;
        for (int i = 1; i <= columnCount; i++) {
            if (rsmd.getColumnName(i).toUpperCase().equals(rowContentColumnName.toUpperCase())) {
                rowContentColumnIndex = i;
            }
        }

        while (resultSet.next()) {

            switch (rsmd.getColumnType(rowContentColumnIndex)) {
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                    ObjectMetadata binMetadata = new ObjectMetadata();
                    s3Client.putObject(bucketName, resultSet.getString(rowKeyColumnName), resultSet.getBinaryStream(rowContentColumnName), binMetadata);
                    binMetadata = null;
                    break;
                case Types.SQLXML:
                    throw new IllegalArgumentException("SQLXML Data Type is not supported. You should convert it to BLOB or CLOB");
                default:
                    s3Client.putObject(bucketName, resultSet.getString(rowKeyColumnName), resultSet.getString(rowContentColumnName));
                    break;
            }
        }
    }


    private void putCsvToS3(ResultSet resultSet, int taskId, AmazonS3 s3Client, String bucketName) throws SQLException {

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Get sink properties
        Properties s3Props = options.getSinkConnectionParams();

        // Set File Name.
        // It is not possible to append to an existing S3 file, ReplicaDB will generate one file per job
        String keyFileName = s3Props.getProperty("csv.keyFileName");
        //TODO comprobar que el nombre del fichero no es nulo
        if (options.getJobs() > 1)
            keyFileName = keyFileName + "_" + taskId;

        // Setting up Streaming transfer
        int numStreams = 1;
        final StreamTransferManager manager = new StreamTransferManager(bucketName, keyFileName, s3Client)
                .numStreams(numStreams)
                .numUploadThreads(2)
                .queueCapacity(2)
                .partSize(5);

        final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();

        try {
            MultiPartOutputStream outputStream = streams.get(0);

            PrintWriter pw = new PrintWriter(outputStream, true);
            CsvWriter csvWriter = new CsvWriter();
            // Custom user csv options
            setCsvWriterOptions(csvWriter);

            // Write the CSV
            try (CsvAppender csvAppender = csvWriter.append(pw)) {

                // headers, only in the first temporal file.
                if (taskId == 0 && Boolean.parseBoolean(s3Props.getProperty("csv.Header"))) {
                    for (int i = 1; i <= columnCount; i++) {
                        csvAppender.appendField(rsmd.getColumnName(i));
                    }
                    csvAppender.endLine();
                }

                String colValue;
                String[] colValues;

                // lines
                while (resultSet.next()) {
                    colValues = new String[columnCount];

                    // Iterate over the columns of the row
                    for (int i = 1; i <= columnCount; i++) {
                        colValue = resultSet.getString(i);

                        if (!this.options.isSinkDisableEscape() && !resultSet.wasNull())
                            colValues[i - 1] = colValue.replace("\n", "\\n").replace("\r", "\\r");
                        else
                            colValues[i - 1] = colValue;
                    }
                    csvAppender.appendLine(colValues);
                }
            }

            // The stream must be closed once all the data has been written
            pw.close();
            outputStream.close();
        } catch (Exception e) {
            // Aborts all uploads
            manager.abort(e);
        }

        // Finishing off
        manager.complete();

    }


    @Override
    protected void createStagingTable() {
    }

    @Override
    protected void mergeStagingTable() {/*Not implemented*/}

    @Override
    public void preSourceTasks() {/*Not implemented*/}

    @Override
    public void postSourceTasks() {/*Not implemented*/}


    @Override
    public void postSinkTasks() {/*Not implemented*/}

    @Override
    public void cleanUp() {/*Not implemented*/}


    /**
     * Retrieves and sets the custom options that define the CSV Writer
     *
     * @param writer
     */
    private void setCsvWriterOptions(CsvWriter writer) {
        Properties csvProperties = options.getSinkConnectionParams();

        String fieldSeparator, textDelimiter, lineDelimiter;

        fieldSeparator = csvProperties.getProperty("csv.FieldSeparator");
        textDelimiter = csvProperties.getProperty("csv.TextDelimiter");
        lineDelimiter = csvProperties.getProperty("csv.LineDelimiter");
        boolean alwaysDelimitText = Boolean.parseBoolean(csvProperties.getProperty("csv.AlwaysDelimitText"));

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


}

