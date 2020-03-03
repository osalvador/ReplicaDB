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

import java.io.PrintWriter;
import java.net.URI;
import java.sql.*;
import java.util.List;
import java.util.Properties;

public class S3Manager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(S3Manager.class.getName());

    private String accessKey;
    private String secretKey;
    private boolean secuereConnection;
    private boolean objectPerRow;
    private String rowKeyColumnName;
    private String rowContentColumnName;
    private String csvKeyFileName;
    private char csvFieldSeparator;
    private char csvTextDelimiter;
    private String csvLineDelimiter;
    private boolean csvAlwaysDelimitText;
    private boolean csvHeader;

    private void setAccessKey(String accessKey) {
        if (accessKey != null && !accessKey.isEmpty())
            this.accessKey = accessKey;
        else
            throw new IllegalArgumentException("accessKey property cannot be null");
    }

    private void setSecretKey(String secretKey) {
        if (secretKey != null && !secretKey.isEmpty())
            this.secretKey = secretKey;
        else
            throw new IllegalArgumentException("secretKey property cannot be null");
    }

    private void setSecuereConnection(String secuereConnection) {
        // Secure connection default true
        if (secuereConnection != null && !secuereConnection.isEmpty())
            this.secuereConnection = Boolean.parseBoolean(secuereConnection);
        else
            this.secuereConnection = true;
    }

    private void setObjectPerRow(boolean objectPerRow) {
        this.objectPerRow = objectPerRow;
    }

    private void setRowKeyColumnName(String rowKeyColumnName) {
        if (rowKeyColumnName != null && !rowKeyColumnName.isEmpty())
            this.rowKeyColumnName = rowKeyColumnName;
        else
            throw new IllegalArgumentException("row.keyColumn property cannot be null");
    }

    private void setRowContentColumnName(String rowContentColumnName) {
        if (rowContentColumnName != null && !rowContentColumnName.isEmpty())
            this.rowContentColumnName = rowContentColumnName;
        else
            throw new IllegalArgumentException("row.contentColumn property cannot be null");
    }

    private void setCsvKeyFileName(String csvKeyFileName) {
        if (csvKeyFileName != null && !csvKeyFileName.isEmpty())
            this.csvKeyFileName = csvKeyFileName;
        else
            throw new IllegalArgumentException("csv.keyFileName property cannot be null");
    }

    private void setCsvFieldSeparator(String csvFieldSeparator) {
        if (csvFieldSeparator != null && csvFieldSeparator.length() > 1)
            throw new IllegalArgumentException("FieldSeparator must be a single char");

        if (csvFieldSeparator != null && !csvFieldSeparator.isEmpty())
            this.csvFieldSeparator = csvFieldSeparator.charAt(0);
    }


    private void setCsvTextDelimiter(String csvTextDelimiter) {
        if (csvTextDelimiter != null && csvTextDelimiter.length() > 1)
            throw new IllegalArgumentException("TextDelimiter must be a single char");

        if (csvTextDelimiter != null && !csvTextDelimiter.isEmpty())
            this.csvTextDelimiter = csvTextDelimiter.charAt(0);
    }

    private void setCsvLineDelimiter(String csvLineDelimiter) {
        this.csvLineDelimiter = csvLineDelimiter;
    }

    private void setCsvAlwaysDelimitText(boolean csvAlwaysDelimitText) {
        this.csvAlwaysDelimitText = csvAlwaysDelimitText;
    }

    private void setCsvHeader(boolean csvHeader) {
        this.csvHeader = csvHeader;
    }

    private boolean isSecuereConnection() {
        return secuereConnection;
    }

    private boolean isObjectPerRow() {
        return objectPerRow;
    }

    private boolean isCsvHeader() {
        return csvHeader;
    }


    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public S3Manager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        loadS3CustomProperties();
    }

    /**
     * Load ReplicaDB custom options for Amazon S3
     */
    private void loadS3CustomProperties() {
        Properties s3Props;

        if (dsType == DataSourceType.SOURCE)
            s3Props = options.getSourceConnectionParams();
        else
            s3Props = options.getSinkConnectionParams();

        // S3 Client properties
        setAccessKey(s3Props.getProperty("accessKey"));
        setSecretKey(s3Props.getProperty("secretKey"));
        setSecuereConnection(s3Props.getProperty("secure-connection"));

        // S3 ReplicaDB upload Mode
        setObjectPerRow(Boolean.parseBoolean(s3Props.getProperty("row.isObject")));

        if (isObjectPerRow()) {
            // One object per row
            setRowKeyColumnName(s3Props.getProperty("row.keyColumn"));
            setRowContentColumnName(s3Props.getProperty("row.contentColumn"));
        } else {
            // One CSV for all rows
            setCsvKeyFileName(s3Props.getProperty("csv.keyFileName"));
            setCsvFieldSeparator(s3Props.getProperty("csv.FieldSeparator"));
            setCsvTextDelimiter(s3Props.getProperty("csv.TextDelimiter"));
            setCsvLineDelimiter(s3Props.getProperty("csv.LineDelimiter"));
            setCsvHeader(Boolean.parseBoolean(s3Props.getProperty("csv.Header")));
            setCsvAlwaysDelimitText(Boolean.parseBoolean(s3Props.getProperty("csv.AlwaysDelimitText")));
        }

    }

    @Override
    public String toString() {
        return "S3Manager{" +
                "accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", secuereConnection=" + secuereConnection +
                ", objectPerRow=" + objectPerRow +
                ", rowKeyColumnName='" + rowKeyColumnName + '\'' +
                ", rowContentColumnName='" + rowContentColumnName + '\'' +
                ", csvKeyFileName='" + csvKeyFileName + '\'' +
                ", csvFieldSeparator=" + csvFieldSeparator +
                ", csvTextDelimiter=" + csvTextDelimiter +
                ", csvLineDelimiter='" + csvLineDelimiter + '\'' +
                ", csvAlwaysDelimitText=" + csvAlwaysDelimitText +
                ", csvHeader=" + csvHeader +
                '}';
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

        URI s3Uri = new URI(options.getSinkConnect());
        // Get Service Endpoint
        // Secure by default
        String serviceEndpoint = "https://";
        if (!isSecuereConnection()) {
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
        String bucketName = s3Uri.getPath().replaceAll("/$", "");

        LOG.debug(this.toString());

        // S3 Client
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withRegion("")
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();


        if (!isObjectPerRow()) {
            // All rows are only one CSV object in s3
            putCsvToS3(resultSet, taskId, s3Client, bucketName);
        } else {
            // Each row is a different object in s3
            putObjectToS3(resultSet, taskId, s3Client, bucketName);
        }

        return 0;

    }

    private void putObjectToS3(ResultSet resultSet, int taskId, AmazonS3 s3Client, String bucketName) throws SQLException {

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Get content column index
        int rowContentColumnIndex = 0;
        for (int i = 1; i <= columnCount; i++) {
            if (rsmd.getColumnName(i).toUpperCase().equals(rowContentColumnName.toUpperCase())) {
                rowContentColumnIndex = i;
            }
        }

        ObjectMetadata binMetadata = new ObjectMetadata();

        while (resultSet.next()) {

            switch (rsmd.getColumnType(rowContentColumnIndex)) {
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                    s3Client.putObject(bucketName, resultSet.getString(rowKeyColumnName), resultSet.getBinaryStream(rowContentColumnName), binMetadata);
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

        // Set File Name.
        // It is not possible to append to an existing S3 file, ReplicaDB will generate one file per job
        // renaming file name with the taskid
        String keyFileName = csvKeyFileName;
        if (options.getJobs() > 1)
            keyFileName = keyFileName.substring(0, keyFileName.lastIndexOf(".")) + "_" + taskId + keyFileName.substring(keyFileName.lastIndexOf("."));

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
            if ((int) csvFieldSeparator != 0) csvWriter.setFieldSeparator(csvFieldSeparator);
            if ((int) csvTextDelimiter != 0) csvWriter.setTextDelimiter(csvTextDelimiter);
            if (csvLineDelimiter != null && !csvLineDelimiter.isEmpty())
                csvWriter.setLineDelimiter(csvLineDelimiter.toCharArray());
            csvWriter.setAlwaysDelimitText(csvAlwaysDelimitText);

            // Write the CSV
            try (CsvAppender csvAppender = csvWriter.append(pw)) {

                // Put headers in the file
                if (isCsvHeader()) {
                    for (int i = 1; i <= columnCount; i++) {
                        csvAppender.appendField(rsmd.getColumnName(i));
                    }
                    csvAppender.endLine();
                }

                String colValue;
                String[] colValues;

                // lines
                if (resultSet.next()) {
                    // Create Bandwidth Throttling
                    bandwidthThrottlingCreate(resultSet, rsmd);
                    do {
                        bandwidthThrottlingAcquiere();

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
                    } while (resultSet.next());
                }
            }

            // The stream must be closed once all the data has been written
            pw.close();
            outputStream.close();
        } catch (Exception e) {
            // Aborts all uploads
            throw manager.abort(e);
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


}

