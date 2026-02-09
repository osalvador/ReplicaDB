package org.replicadb.db2;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.config.ReplicadbLocalStackContainer;
import org.replicadb.manager.file.FileFormats;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22S3FileTest {
    private static final Logger LOG = LogManager.getLogger(DB22S3FileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    private Connection db2Conn;
    private AmazonS3 s3Client;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    private static ReplicadbLocalStackContainer localstack;

    @BeforeAll
    static void setUp() {
        localstack = ReplicadbLocalStackContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.s3Client = localstack.createS3Client();
    }

    @AfterEach
    void tearDown() throws SQLException {
        List<S3ObjectSummary> objects = s3Client.listObjects(ReplicadbLocalStackContainer.TEST_BUCKET_NAME).getObjectSummaries();
        for (S3ObjectSummary obj : objects) {
            s3Client.deleteObject(ReplicadbLocalStackContainer.TEST_BUCKET_NAME, obj.getKey());
        }
        this.db2Conn.close();
    }

    @Test
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testS3Connection() {
        assertTrue(s3Client.doesBucketExist(ReplicadbLocalStackContainer.TEST_BUCKET_NAME));
    }

    @Test
    void testDb2SourceRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testDb22S3FileComplete() throws ParseException, IOException {
        String s3Url = localstack.getS3ConnectionString() + "/db22s3_test.csv";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
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
        LOG.info("S3 objects created: {}", objects.size());
    }

    @Test
    void testDb22S3FileCompleteParallel() throws ParseException, IOException {
        String s3Url = localstack.getS3ConnectionString() + "/db22s3_parallel_test.csv";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
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
        LOG.info("S3 objects created: {}", objects.size());
    }
}
