package org.replicadb.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.replicadb.server.config.PostgresTestcontainersConfig;

@SpringBootTest
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
@Tag("docker")
class ReplicaDbServerApplicationTest {

    @Test
    void contextLoads() {
    }
}
