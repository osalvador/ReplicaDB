package org.replicadb.server.security;

import org.junit.jupiter.api.Test;
import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@SpringJUnitConfig(WithMockReplicaDbUserTest.TestConfiguration.class)
class WithMockReplicaDbUserTest {

    @Test
    @WithMockReplicaDbUser(role = org.replicadb.server.security.domain.GlobalRole.VIEWER,
            userId = "00000000-0000-0000-0000-000000000007")
    void createsReplicaDbPrincipal() {
        ReplicaDbUserDetails details = assertInstanceOf(ReplicaDbUserDetails.class,
                SecurityContextHolder.getContext().getAuthentication().getPrincipal());

        assertEquals("00000000-0000-0000-0000-000000000007", details.userId().toString());
        assertEquals("VIEWER", details.appUser().role().name());
    }

    @Configuration
    static class TestConfiguration {
    }
}
