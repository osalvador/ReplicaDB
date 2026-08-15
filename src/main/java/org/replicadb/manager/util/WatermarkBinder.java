package org.replicadb.manager.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

/**
 * Resolves the JDBC type of a declared incremental-watermark column and converts
 * the raw CLI/options-file watermark value into a typed bind parameter, so the
 * watermark predicate is always bound rather than concatenated into SQL.
 */
public final class WatermarkBinder {

    private static final Logger LOG = LogManager.getLogger(WatermarkBinder.class.getName());

    private WatermarkBinder() {
    }

    public static int resolveColumnType(List<ColumnDescriptor> columnDescriptors, String columnName) {
        if (columnDescriptors != null) {
            for (ColumnDescriptor descriptor : columnDescriptors) {
                if (descriptor.getColumnName() != null && descriptor.getColumnName().equalsIgnoreCase(columnName)) {
                    return descriptor.getJdbcType();
                }
            }
        }
        throw new IllegalArgumentException("Watermark column '" + columnName + "' was not found in source metadata");
    }

    public static Object convertToBoundValue(String rawValue, int jdbcType) {
        try {
            switch (jdbcType) {
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return Timestamp.valueOf(rawValue);
                case Types.DATE:
                    return Date.valueOf(rawValue);
                case Types.TIME:
                    return Time.valueOf(rawValue);
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.REAL:
                    return new BigDecimal(rawValue);
                default:
                    return rawValue;
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Cannot convert incremental-watermark-value '" + rawValue + "' to the watermark column's SQL type", e);
        }
    }

    public static int compareCandidates(String left, String right, int jdbcType) {
        try {
            switch (jdbcType) {
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return Timestamp.valueOf(left).compareTo(Timestamp.valueOf(right));
                case Types.DATE:
                    return Date.valueOf(left).compareTo(Date.valueOf(right));
                case Types.TIME:
                    return Time.valueOf(left).compareTo(Time.valueOf(right));
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.REAL:
                    return new BigDecimal(left).compareTo(new BigDecimal(right));
                default:
                    return left.compareTo(right);
            }
        } catch (IllegalArgumentException e) {
            LOG.warn("Could not compare watermark candidates '{}' and '{}' as SQL type {}; falling back to lexicographic comparison",
                    left, right, jdbcType);
            return left.compareTo(right);
        }
    }
}
