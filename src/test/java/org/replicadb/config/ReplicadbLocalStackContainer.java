package org.replicadb.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class ReplicadbLocalStackContainer extends LocalStackContainer {
    private static final Logger LOG = LogManager.getLogger(ReplicadbLocalStackContainer.class);
    private static final DockerImageName IMAGE_VERSION = DockerImageName.parse("localstack/localstack:3.0");
    public static final String TEST_BUCKET_NAME = "replicadb-test";
    private static ReplicadbLocalStackContainer container;

    private ReplicadbLocalStackContainer() {
        super(IMAGE_VERSION);
        withServices(S3);
    }

    public static ReplicadbLocalStackContainer getInstance() {
        if (container == null) {
            container = new ReplicadbLocalStackContainer();
            container.start();
        }
        return container;
    }

    @Override
    public void start() {
        super.start();
        
        // Create test bucket
        try {
            AmazonS3 s3Client = createS3Client();
            LOG.info("Creating S3 test bucket: {}", TEST_BUCKET_NAME);
            s3Client.createBucket(TEST_BUCKET_NAME);
            LOG.info("S3 test bucket created successfully");
        } catch (Exception e) {
            LOG.error("Failed to create S3 test bucket", e);
            throw new RuntimeException("Failed to initialize LocalStack S3", e);
        }
    }

    @Override
    public void stop() {
        // Do nothing, JVM handles shut down
    }

    /**
     * Creates an AmazonS3 client configured for LocalStack.
     */
    public AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        getEndpointOverride(S3).toString(),
                        getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(getAccessKey(), getSecretKey())))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    /**
     * Returns the S3 connection string for ReplicaDB sink configuration.
     * Format: s3://endpoint/bucket
     */
    public String getS3ConnectionString() {
        URI endpoint = getEndpointOverride(S3);
        return "s3://" + endpoint.getHost() + ":" + endpoint.getPort() + "/" + TEST_BUCKET_NAME;
    }

    /**
     * Returns the endpoint URL for S3 operations.
     */
    public URI getS3Endpoint() {
        return getEndpointOverride(S3);
    }
}
