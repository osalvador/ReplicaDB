package org.replicadb.mysql;

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
import org.replicadb.config.ReplicadbMysqlContainer;
import org.replicadb.manager.file.FileFormats;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2S3FileTest {
    private static final Logger LOG = LogManager.getLogger(MySQL2S3FileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection mysqlConn;
    private AmazonS3 s3Client;

    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    private static ReplicadbLocalStackContainer localstack;

    @BeforeAll
    static void setUp() {
        localstack = ReplicadbLocalStackContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        this.s3Client = localstack.createS3Client();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Delete all objects in the test bucket
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        for (S3ObjectSummary obj : objects) {
            s3Client.deleteObject(ReplicadbLocalStackContainer.TEST_BUCKET_NAME, obj.getKey());
        }
        this.mysqlConn.close();
    }

    @Test
    void testMysqlVersion56() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("5.6"));
    }

    @Test
    void testS3Connection() {
        assertTrue(s3Client.doesBucketExist(ReplicadbLocalStackContainer.TEST_BUCKET_NAME));
    }

    @Test
    void testMysqlSourceRows() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testMySQL2S3FileComplete() throws ParseException, IOException {
        String s3Url = localstack.getS3ConnectionString() + "/mysql2s3_test.csv";
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", s3Url,
                "--sink-file-format", FileFormats.CSV.getType()
        };
        ToolOptions options = new ToolOptions(args);
        
        // Set S3 credentials for LocalStack
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("accessKey", localstack.getAccessKey());
        sinkConnectionParams.setProperty("secretKey", localstack.getSecretKey());
        sinkConnectionParams.setProperty("secure-connection", "false");
        options.setSinkConnectionParams(sinkConnectionParams);
        
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify file was created in S3
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        assertTrue(objects.size() > 0, "S3 bucket should contain at least one object");
        LOG.info("S3 objects created: {}", objects.size());
    }

    @Test
    void testMySQL2S3FileCompleteParallel() throws ParseException, IOException {
        String s3Url = localstack.getS3ConnectionString() + "/mysql2s3_parallel_test.csv";
        
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", s3Url,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        
        // Set S3 credentials for LocalStack
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("accessKey", localstack.getAccessKey());
        sinkConnectionParams.setProperty("secretKey", localstack.getSecretKey());
        sinkConnectionParams.setProperty("secure-connection", "false");
        options.setSinkConnectionParams(sinkConnectionParams);
        
        assertEquals(0, ReplicaDB.processReplica(options));
        
        // Verify files were created in S3
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        assertTrue(objects.size() > 0, "S3 bucket should contain at least one object");
        LOG.info("S3 objects created: {}", objects.size());
    }
}
