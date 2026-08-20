package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LeaseTokenTest {

    @Test
    void generatesDistinctOpaqueValues() {
        LeaseToken first = LeaseToken.generate();
        LeaseToken second = LeaseToken.generate();

        assertNotNull(first.value());
        assertNotNull(first.toString());
        assertNotEquals(first, second);
    }

    @Test
    void rejectsNullValue() {
        assertThrows(NullPointerException.class, () -> new LeaseToken(null));
    }
}
