package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PageRequestParamsTest {

    @Test
    void appliesDefaultsWhenParametersAreAbsent() {
        PageRequestParams params = PageRequestParams.of(null, null);

        assertEquals(0, params.page());
        assertEquals(50, params.size());
    }

    @Test
    void rejectsNegativePages() {
        assertThrows(IllegalArgumentException.class, () -> PageRequestParams.of(-1, 50));
    }

    @Test
    void clampsSizeToTheSupportedBounds() {
        assertEquals(1, PageRequestParams.of(0, 0).size());
        assertEquals(1, PageRequestParams.of(0, -5).size());
        assertEquals(200, PageRequestParams.of(0, 500).size());
        assertEquals(200, PageRequestParams.of(0, 200).size());
    }
}
