package org.replicadb.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LocalStackContainerTest {
    @Test
    void testLocalStackStarts() {
        ReplicadbLocalStackContainer localstack = ReplicadbLocalStackContainer.getInstance();
        assertNotNull(localstack);
        assertTrue(localstack.isRunning());
        assertNotNull(localstack.getS3ConnectionString());
        System.out.println("S3 Connection: " + localstack.getS3ConnectionString());
    }
}
