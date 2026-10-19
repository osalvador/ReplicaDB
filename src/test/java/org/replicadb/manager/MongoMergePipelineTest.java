package org.replicadb.manager;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoMergePipelineTest {

    private static final String SINK = "t_sink";

    @Test
    void nonIdMergeKeyKeepsProjectionStage() {
        String expected = "[ {$project: {_id:0}},{$merge:{ into:\"t_sink\", on:[\"code\"], "
                + "whenMatched: \"replace\", whenNotMatched: \"insert\" }} ]";
        assertEquals(expected, MongoDBManager.buildMergeAggregation(SINK, List.of("code")));
    }

    @Test
    void idMergeKeyOmitsProjectionStage() {
        String expected = "[ {$merge:{ into:\"t_sink\", on:[\"_id\"], "
                + "whenMatched: \"replace\", whenNotMatched: \"insert\" }} ]";
        assertEquals(expected, MongoDBManager.buildMergeAggregation(SINK, List.of("_id")));
    }

    @Test
    void compoundKeyContainingIdOmitsProjectionStage() {
        String query = MongoDBManager.buildMergeAggregation(SINK, List.of("_id", "code"));
        assertFalse(query.contains("$project"));
        assertTrue(query.contains("on:[\"_id\",\"code\"]"));
    }
}
