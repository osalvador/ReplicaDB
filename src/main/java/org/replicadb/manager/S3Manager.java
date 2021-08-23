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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.file.FileManager;
import org.replicadb.manager.file.FileManagerFactory;

import java.net.URI;
import java.sql.*;
import java.util.Arrays;
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
    private String keyFileName;
    private final FileManager fileManager;

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

    private void setKeyFileName(String keyFileName) {
        if (keyFileName != null && !keyFileName.isEmpty())
            this.keyFileName = keyFileName;
        /*else
            throw new IllegalArgumentException("csv.keyFileName property cannot be null");*/
    }

    private boolean isSecuereConnection() {
        return secuereConnection;
    }

    private boolean isObjectPerRow() {
        return objectPerRow;
    }

    // TODO: complete-atomic and incremental? is not supported in s3

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public S3Manager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        loadS3CustomProperties();

        if (!isObjectPerRow())
            this.fileManager = new FileManagerFactory().accept(opts, dsType);
        else
            this.fileManager = null;
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
            // One File for all rows
            setKeyFileName(s3Props.getProperty("keyFileName"));
        }

    }

    @Override
    public String toString() {
        return "S3Manager{" +
                "accessKey='" + accessKey + '\'' +
                ", secretKey='" + "****" + '\'' +
                ", secuereConnection=" + secuereConnection +
                ", objectPerRow=" + objectPerRow +
                ", rowKeyColumnName='" + rowKeyColumnName + '\'' +
                ", rowContentColumnName='" + rowContentColumnName + '\'' +
                ", keyFileName='" + keyFileName + '\'' +
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
        if (keyFileName == null) {
            this.keyFileName = bucketName.substring(bucketName.lastIndexOf("/") + 1);
            bucketName = bucketName.replace(keyFileName,"").replaceAll("/$", "");
        }
        LOG.info("Bucket Name: {}, File Name: {}", bucketName, keyFileName);

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
            // All rows are only one File object in s3
            return putFileToS3(resultSet, taskId, s3Client, bucketName);
        } else {
            // Each row is a different object in s3
            return putObjectToS3(resultSet, taskId, s3Client, bucketName);
        }

    }

    private int putObjectToS3(ResultSet resultSet, int taskId, AmazonS3 s3Client, String bucketName) throws SQLException {

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
        int processedRows = 0;
        while (resultSet.next()) {

            switch (rsmd.getColumnType(rowContentColumnIndex)) {
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                    s3Client.putObject(bucketName, resultSet.getString(rowKeyColumnName), resultSet.getBinaryStream(rowContentColumnName), binMetadata);
                    break;
                case Types.SQLXML:
                    SQLXML sqlxmlData = resultSet.getSQLXML(rowContentColumnName);
                    s3Client.putObject(bucketName, resultSet.getString(rowKeyColumnName), sqlxmlData.getBinaryStream(), binMetadata);
                    sqlxmlData.free();
                default:
                    s3Client.putObject(bucketName, resultSet.getString(rowKeyColumnName), resultSet.getString(rowContentColumnName));
                    break;
            }
            processedRows++;
        }
        return processedRows;
    }

    private int putFileToS3(ResultSet resultSet, int taskId, AmazonS3 s3Client, String bucketName) throws SQLException {

        // Set File Name.
        // It is not possible to append to an existing S3 file, ReplicaDB will generate one file per job
        // renaming file name with the taskid
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
        int processedRows = 0;

        try {
            MultiPartOutputStream outputStream = streams.get(0);
            // Write data to the specific file manager
            processedRows = this.fileManager.writeData(outputStream, resultSet, taskId, null);
            outputStream.close();
        } catch (Exception e) {
            // Aborts all uploads
            throw manager.abort(e);
        }

        // Finishing off
        manager.complete();
        return processedRows;

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
    public void cleanUp() throws Exception {
        if (fileManager != null)
            this.fileManager.cleanUp();
    }

}

