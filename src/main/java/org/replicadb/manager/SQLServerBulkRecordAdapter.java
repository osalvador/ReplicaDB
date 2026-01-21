package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.RowSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Adapter class that wraps any RowSet to be used with SQLServerBulkCopy.
 * This adapter handles type conversions that SQLServerBulkCopy doesn't support natively,
 * such as converting Boolean to Integer (0/1) for BIT columns.
 */
public class SQLServerBulkRecordAdapter implements ISQLServerBulkRecord {

    private static final Logger LOG = LogManager.getLogger(SQLServerBulkRecordAdapter.class);

    private final RowSet rowSet;
    private final ResultSetMetaData metaData;
    private final int columnCount;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter timeFormatter;

    /**
     * Creates a new adapter wrapping the given RowSet.
     *
     * @param rowSet the RowSet to wrap
     * @throws SQLException if metadata cannot be retrieved
     */
    public SQLServerBulkRecordAdapter(RowSet rowSet) throws SQLException {
        this.rowSet = rowSet;
        this.metaData = rowSet.getMetaData();
        this.columnCount = metaData.getColumnCount();
        LOG.debug("Created SQLServerBulkRecordAdapter with {} columns", columnCount);
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        Set<Integer> ordinals = new LinkedHashSet<>();
        for (int i = 1; i <= columnCount; i++) {
            ordinals.add(i);
        }
        return ordinals;
    }

    @Override
    public String getColumnName(int column) {
        try {
            return metaData.getColumnName(column);
        } catch (SQLException e) {
            LOG.error("Error getting column name for column {}", column, e);
            return null;
        }
    }

    @Override
    public int getColumnType(int column) {
        try {
            int type = metaData.getColumnType(column);
            // Convert BOOLEAN type to BIT for SQL Server compatibility
            if (type == Types.BOOLEAN) {
                return Types.BIT;
            }
            return type;
        } catch (SQLException e) {
            LOG.error("Error getting column type for column {}", column, e);
            return Types.VARCHAR;
        }
    }

    @Override
    public int getPrecision(int column) {
        try {
            int precision = metaData.getPrecision(column);
            // SQL Server BulkCopy requires non-zero precision for variable-length types
            if (precision <= 0) {
                int type = getColumnType(column);
                switch (type) {
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.LONGVARCHAR:
                        return 8000; // SQL Server VARCHAR max without MAX
                    case Types.NVARCHAR:
                    case Types.NCHAR:
                    case Types.LONGNVARCHAR:
                        return 4000; // SQL Server NVARCHAR max without MAX
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return 8000; // SQL Server VARBINARY max without MAX
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        return 38; // SQL Server max precision for DECIMAL
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.REAL:
                        return 53; // SQL Server FLOAT precision
                    default:
                        return 38; // Safe default
                }
            }
            return precision;
        } catch (SQLException e) {
            LOG.error("Error getting precision for column {}", column, e);
            return 38; // Safe default instead of 0
        }
    }

    @Override
    public int getScale(int column) {
        try {
            int scale = metaData.getScale(column);
            // For DECIMAL/NUMERIC with 0 scale, keep it (it's valid for integers)
            // Only provide default if we can't get it
            return scale;
        } catch (SQLException e) {
            LOG.error("Error getting scale for column {}", column, e);
            return 0; // 0 is valid for scale
        }
    }

    @Override
    public boolean isAutoIncrement(int column) {
        try {
            return metaData.isAutoIncrement(column);
        } catch (SQLException e) {
            LOG.error("Error checking auto increment for column {}", column, e);
            return false;
        }
    }

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        // Return null to use default formatting
        return null;
    }

    @Override
    public void setTimestampWithTimezoneFormat(String format) {
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter formatter) {
        this.dateTimeFormatter = formatter;
    }

    @Override
    public void setTimeWithTimezoneFormat(String format) {
        this.timeFormatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter formatter) {
        this.timeFormatter = formatter;
    }

    @Override
    public void addColumnMetadata(int positionInFile, String columnName, int jdbcType, int precision, int scale) {
        // Not needed - we read metadata from the RowSet
        LOG.trace("addColumnMetadata called for column {} at position {}", columnName, positionInFile);
    }

    @Override
    public void addColumnMetadata(int positionInFile, String columnName, int jdbcType, int precision, int scale, DateTimeFormatter dateTimeFormatter) {
        // Not needed - we read metadata from the RowSet
        LOG.trace("addColumnMetadata with formatter called for column {} at position {}", columnName, positionInFile);
    }

    @Override
    public Object[] getRowData() {
        try {
            Object[] rowData = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                Object value = rowSet.getObject(i);
                int columnType = getColumnType(i);

                // Convert Boolean to Integer (0/1) for SQL Server BIT compatibility
                if (value instanceof Boolean) {
                    value = ((Boolean) value) ? 1 : 0;
                }
                // Convert BigDecimal to appropriate numeric type for SQL Server
                else if (value instanceof java.math.BigDecimal) {
                    java.math.BigDecimal bd = (java.math.BigDecimal) value;
                    switch (columnType) {
                        case Types.BIGINT:
                            value = bd.longValue();
                            break;
                        case Types.INTEGER:
                            value = bd.intValue();
                            break;
                        case Types.SMALLINT:
                        case Types.TINYINT:
                            value = bd.shortValue();
                            break;
                        case Types.FLOAT:
                        case Types.DOUBLE:
                            value = bd.doubleValue();
                            break;
                        case Types.REAL:
                            value = bd.floatValue();
                            break;
                        // For DECIMAL/NUMERIC, keep as BigDecimal
                        default:
                            break;
                    }
                }

                rowData[i - 1] = value;
            }
            return rowData;
        } catch (SQLException e) {
            LOG.error("Error getting row data", e);
            throw new RuntimeException("Error getting row data from RowSet", e);
        }
    }

    @Override
    public boolean next() {
        try {
            return rowSet.next();
        } catch (SQLException e) {
            LOG.error("Error advancing to next row", e);
            throw new RuntimeException("Error advancing to next row in RowSet", e);
        }
    }
}
