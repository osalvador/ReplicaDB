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
            return metaData.getPrecision(column);
        } catch (SQLException e) {
            LOG.error("Error getting precision for column {}", column, e);
            return 0;
        }
    }

    @Override
    public int getScale(int column) {
        try {
            return metaData.getScale(column);
        } catch (SQLException e) {
            LOG.error("Error getting scale for column {}", column, e);
            return 0;
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

                // Convert Boolean to Integer (0/1) for SQL Server BIT compatibility
                if (value instanceof Boolean) {
                    value = ((Boolean) value) ? 1 : 0;
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
