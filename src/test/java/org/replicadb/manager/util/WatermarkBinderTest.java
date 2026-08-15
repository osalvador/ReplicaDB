package org.replicadb.manager.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkBinderTest {

    @Test
    void resolveColumnTypeFindsColumnCaseInsensitively() {
        List<ColumnDescriptor> descriptors = List.of(
                new ColumnDescriptor("C_INTEGER", Types.INTEGER, 10, 0, 1),
                new ColumnDescriptor("C_TIMESTAMP", Types.TIMESTAMP, 0, 0, 1));

        assertEquals(Types.INTEGER, WatermarkBinder.resolveColumnType(descriptors, "c_integer"));
        assertEquals(Types.TIMESTAMP, WatermarkBinder.resolveColumnType(descriptors, "C_TIMESTAMP"));
    }

    @Test
    void resolveColumnTypeThrowsForUnknownName() {
        List<ColumnDescriptor> descriptors = List.of(new ColumnDescriptor("C_INTEGER", Types.INTEGER, 10, 0, 1));

        assertThrows(IllegalArgumentException.class, () -> WatermarkBinder.resolveColumnType(descriptors, "missing"));
    }

    @Test
    void convertToBoundValueProducesBigDecimalForNumericTypes() {
        assertEquals(new java.math.BigDecimal("42"), WatermarkBinder.convertToBoundValue("42", Types.INTEGER));
        assertEquals(new java.math.BigDecimal("42.5"), WatermarkBinder.convertToBoundValue("42.5", Types.NUMERIC));
    }

    @Test
    void convertToBoundValueProducesTimestampForTimestampType() {
        assertEquals(java.sql.Timestamp.valueOf("2024-01-01 10:00:00"),
                WatermarkBinder.convertToBoundValue("2024-01-01 10:00:00", Types.TIMESTAMP));
    }

    @Test
    void convertToBoundValueProducesDateForDateType() {
        assertEquals(java.sql.Date.valueOf("2024-01-01"), WatermarkBinder.convertToBoundValue("2024-01-01", Types.DATE));
    }

    @Test
    void convertToBoundValueProducesStringForVarcharType() {
        assertEquals("abc", WatermarkBinder.convertToBoundValue("abc", Types.VARCHAR));
    }

    @Test
    void convertToBoundValueThrowsForNonNumericStringAgainstBigint() {
        assertThrows(IllegalArgumentException.class, () -> WatermarkBinder.convertToBoundValue("not-a-number", Types.BIGINT));
    }

    @Test
    void compareCandidatesOrdersNumericStringsNotLexicographically() {
        assertTrue(WatermarkBinder.compareCandidates("9", "10", Types.INTEGER) < 0);
        assertTrue(WatermarkBinder.compareCandidates("10", "9", Types.INTEGER) > 0);
    }

    @Test
    void compareCandidatesOrdersTimestampsCorrectly() {
        assertTrue(WatermarkBinder.compareCandidates("2024-01-01 00:00:00", "2024-01-02 00:00:00", Types.TIMESTAMP) < 0);
    }

    @Test
    void compareCandidatesFallsBackToLexicographicAndLogsWarningOnParseFailure() {
        List<LogEvent> capturedEvents = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("test-appender", null, null, false, null) {
            @Override
            public void append(LogEvent event) {
                capturedEvents.add(event.toImmutable());
            }
        };
        appender.start();

        Logger logger = (Logger) LogManager.getLogger(WatermarkBinder.class.getName());
        logger.addAppender(appender);
        try {
            int result = WatermarkBinder.compareCandidates("not-a-number", "42", Types.INTEGER);
            assertEquals("not-a-number".compareTo("42"), result);
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }

        assertTrue(capturedEvents.stream().anyMatch(event -> event.getLevel() == Level.WARN
                && event.getMessage().getFormattedMessage().contains("falling back to lexicographic comparison")));
    }
}
