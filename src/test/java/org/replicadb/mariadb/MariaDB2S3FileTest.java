package org.replicadb.mariadb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbLocalStackContainer;
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.replicadb.manager.file.FileFormats;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MariaDB2S3FileTest {
    private static final Logger LOG = LogManager.getLogger(MariaDB2S3FileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private Connection mariadbConn;
    private AmazonS3 s3Client;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    private static ReplicadbLocalStackContainer localstack;

    @BeforeAll
    static void setUp() {
        localstack = ReplicadbLocalStackContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
        this.s3Client = localstack.createS3Client();
    }

    @AfterEach
    void tearDown() throws SQLException {
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        for (S3ObjectSummary obj : objects) {
            s3Client.deleteObject(ReplicadbLocalStackContainer.TEST_BUCKET_NAME, obj.getKey());
        }
        this.mariadbConn.close();
    }

    private int countCsvRows(String bucketName) throws IOException {
        List<S3ObjectSummary> objects = s3Client.listObjects(bucketName).getObjectSummaries();
        if (objects.isEmpty()) {
            return 0;
        }
        
        // Count total rows from ALL CSV files in the bucket
        int totalRows = 0;
        for (S3ObjectSummary obj : objects) {
            String objectKey = obj.getKey();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3Client.getObject(bucketName, objectKey).getObjectContent()))) {
                int lineCount = 0;
                while (reader.readLine() != null) {
                    lineCount++;
                }
                // Subtract 1 for header row (first line)
                totalRows += Math.max(0, lineCount - 1);
            }
        }
        return totalRows;
    }

    @Test
    void testMariadbVersion102() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        assertTrue(version.contains("10.2"));
    }

    @Test
    void testS3Connection() {
        assertTrue(s3Client.doesBucketExist(ReplicadbLocalStackContainer.TEST_BUCKET_NAME));
    }

    @Test
    void testMariaDB2S3FileComplete() throws ParseException, IOException {
        // Clean bucket before test
        List<S3ObjectSummary> existingObjects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        for (S3ObjectSummary obj : existingObjects) {
            s3Client.deleteObject(ReplicadbLocalStackContainer.TEST_BUCKET_NAME, obj.getKey());
        }
        
        String s3Url = localstack.getS3ConnectionString() + "/mariadb2s3_test.csv";
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", s3Url,
                "--sink-file-format", FileFormats.CSV.getType()
        };
        ToolOptions options = new ToolOptions(args);
        
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("accessKey", localstack.getAccessKey());
        sinkConnectionParams.setProperty("secretKey", localstack.getSecretKey());
        sinkConnectionParams.setProperty("secure-connection", "false");
        options.setSinkConnectionParams(sinkConnectionParams);
        
        assertEquals(0, ReplicaDB.processReplica(options));
        
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        assertTrue(objects.size() > 0, "S3 bucket should contain at least one object");
        
        int rowCount = countCsvRows(ReplicadbLocalStackContainer.TEST_BUCKET_NAME);
        assertEquals(EXPECTED_ROWS, rowCount, "CSV row count should match EXPECTED_ROWS");
    }

    @Test
    void testMariaDB2S3FileCompleteParallel() throws ParseException, IOException {
        // Clean bucket before test
        List<S3ObjectSummary> existingObjects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        for (S3ObjectSummary obj : existingObjects) {
            s3Client.deleteObject(ReplicadbLocalStackContainer.TEST_BUCKET_NAME, obj.getKey());
        }
        
        String s3Url = localstack.getS3ConnectionString() + "/mariadb2s3_parallel_test.csv";
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", s3Url,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("accessKey", localstack.getAccessKey());
        sinkConnectionParams.setProperty("secretKey", localstack.getSecretKey());
        sinkConnectionParams.setProperty("secure-connection", "false");
        options.setSinkConnectionParams(sinkConnectionParams);
        
        assertEquals(0, ReplicaDB.processReplica(options));
        
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        assertTrue(objects.size() > 0, "S3 bucket should contain at least one object");
        
        int rowCount = countCsvRows(ReplicadbLocalStackContainer.TEST_BUCKET_NAME);
        assertEquals(EXPECTED_ROWS, rowCount, "CSV row count should match EXPECTED_ROWS");
    }
}
