package org.replicadb.manager.db2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.JdbcDrivers;
import org.replicadb.manager.SqlManager;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * DB2-specific connection manager supporting DB2 LUW and DB2/i platforms.
 * Provides ROW_NUMBER-based parallel reads and DB2-specific staging/merge operations.
 */
public class Db2Manager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(Db2Manager.class.getName());

    /**
     * Constructs the Db2Manager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     * @param dsType the data source type for this manager.
     */
    public Db2Manager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    /**
     * Resolves the DB2 driver class based on the JDBC URL scheme.
     *
     * @return the DB2 JDBC driver class name.
     */
    @Override
    public String getDriverClass() {
        String connectStr = dsType == DataSourceType.SOURCE
            ? options.getSourceConnect()
            : options.getSinkConnect();

        if (connectStr != null && connectStr.startsWith(JdbcDrivers.DB2_AS400.getSchemePrefix())) {
            return JdbcDrivers.DB2_AS400.getDriverClass();
        }
        return JdbcDrivers.DB2.getDriverClass();
    }

    /**
     * Reads data from a DB2 table with optional ROW_NUMBER-based partitioning.
     *
     * @param tableName the table name, or null to use the configured source table.
     * @param columns ignored for DB2; columns are resolved from options.
     * @param nThread the thread index for partitioned reads.
     * @return the ResultSet for the requested partition.
     * @throws SQLException if query execution fails.
     */
    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {
        String resolvedTable = tableName == null ? this.options.getSourceTable() : tableName;
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        String baseQuery;
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            baseQuery = "SELECT * FROM (" + options.getSourceQuery() + ") AS SRC";
        } else {
            baseQuery = "SELECT " + allColumns + " FROM " + escapeTableName(resolvedTable);
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                baseQuery = baseQuery + " WHERE " + options.getSourceWhere();
            }
        }

        if (this.options.getJobs() == 1) {
            return super.execute(baseQuery);
        }

        String innerSelectColumns = "*".equals(allColumns) ? "SRC.*" : allColumns;
        String outerSelectColumns = "*".equals(allColumns) ? "*" : allColumns;
        String sqlCmd = "SELECT " + outerSelectColumns + " FROM (SELECT " + innerSelectColumns
            + ", MOD(ROW_NUMBER() OVER (ORDER BY 1), " + this.options.getJobs()
            + ") AS RN FROM (" + baseQuery + ") SRC) PART WHERE RN = " + nThread;

        LOG.debug("{}: Reading table with command: {}", Thread.currentThread().getName(), sqlCmd);
        return super.execute(sqlCmd);
    }

    /**
     * Inserts data into the DB2 sink table or staging table using batch PreparedStatement.
     *
     * @param resultSet the source ResultSet.
     * @param taskId the task identifier for logging.
     * @return total rows inserted.
     * @throws SQLException if insert fails.
     * @throws IOException if data conversion fails.
     */
    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException {
        int totalRows = 0;
        ResultSetMetaData rsmd = resultSet.getMetaData();
        String tableName;

        if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
            tableName = getSinkTableName();
        } else {
            tableName = getQualifiedStagingTableName();
        }

        String allColumns = getAllSinkColumns(rsmd);
        int columnsNumber = rsmd.getColumnCount();
        String[] sinkColumnNames = Arrays.stream(allColumns.split(","))
            .map(name -> name.replace("\"", "").trim())
            .toArray(String[]::new);

        Map<String, Integer> sinkColumnTypes = getSinkColumnTypes(tableName);
        if (LOG.isDebugEnabled()) {
            StringBuilder mappingLog = new StringBuilder("DB2 sink mapping (index: sourceLabel -> sinkColumn/type): ");
            for (int i = 1; i <= columnsNumber; i++) {
                String columnLabel = rsmd.getColumnLabel(i);
                String sinkColumnName = i - 1 < sinkColumnNames.length ? sinkColumnNames[i - 1] : columnLabel;
                int fallbackType = rsmd.getColumnType(i);
                int targetType = getSinkColumnType(sinkColumnTypes, sinkColumnName, fallbackType);
                if (i > 1) {
                    mappingLog.append(" | ");
                }
                mappingLog.append(i)
                    .append(": ")
                    .append(columnLabel)
                    .append(" -> ")
                    .append(sinkColumnName)
                    .append("/")
                    .append(targetType);
            }
            LOG.debug(mappingLog.toString());
        }

        String sqlCdm = getInsertSQLCommand(tableName, allColumns, columnsNumber);
        PreparedStatement ps = this.getConnection().prepareStatement(sqlCdm);

        final int batchSize = options.getFetchSize();
        int count = 0;

        LOG.info("Inserting data with this command: {}", sqlCdm);

        if (resultSet.next()) {
            BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);

            do {
                bt.acquiere();

                for (int i = 1; i <= columnsNumber; i++) {
                    String columnLabel = rsmd.getColumnLabel(i);
                    String sinkColumnName = i - 1 < sinkColumnNames.length ? sinkColumnNames[i - 1] : columnLabel;
                    int fallbackType = rsmd.getColumnType(i);
                    int targetType = getSinkColumnType(sinkColumnTypes, sinkColumnName, fallbackType);
                    switch (targetType) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case -15:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            ps.setString(i, resultSet.getString(i));
                            break;
                        case Types.INTEGER:
                        case Types.TINYINT:
                        case Types.SMALLINT:
                            int intVal = resultSet.getInt(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setInt(i, intVal);
                            }
                            break;
                        case Types.BIGINT:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            java.math.BigDecimal bdVal = resultSet.getBigDecimal(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setBigDecimal(i, bdVal);
                            }
                            break;
                        case Types.DOUBLE:
                            double dblVal = resultSet.getDouble(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setDouble(i, dblVal);
                            }
                            break;
                        case Types.FLOAT:
                            float fltVal = resultSet.getFloat(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setFloat(i, fltVal);
                            }
                            break;
                        case Types.DATE:
                            // Handle SQLite string-based dates that can't be parsed by DB2
                            try {
                                java.sql.Date dateVal = resultSet.getDate(i);
                                ps.setDate(i, dateVal);
                            } catch (SQLException e) {
                                // Fallback: If getDate() fails, try getString() and convert
                                String dateStr = resultSet.getString(i);
                                if (dateStr == null || resultSet.wasNull()) {
                                    ps.setNull(i, targetType);
                                } else {
                                    // Parse string date (YYYY-MM-DD) to java.sql.Date
                                    try {
                                        java.sql.Date parsedDate = java.sql.Date.valueOf(dateStr);
                                        ps.setDate(i, parsedDate);
                                    } catch (IllegalArgumentException iae) {
                                        LOG.warn("Unable to parse date string '{}', setting NULL: {}", dateStr, iae.getMessage());
                                        ps.setNull(i, targetType);
                                    }
                                }
                            }
                            break;
                        case Types.TIMESTAMP:
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        case -101:
                        case -102:
                            java.sql.Timestamp tsVal = resultSet.getTimestamp(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setTimestamp(i, tsVal);
                            }
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            byte[] binBytes = resultSet.getBytes(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setBytes(i, binBytes);
                            }
                            break;
                        case Types.BLOB:
                            byte[] blobBytes = resultSet.getBytes(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setBytes(i, blobBytes);
                            }
                            break;
                        case Types.CLOB:
                            String clobTypeName = rsmd.getColumnTypeName(i);
                            if ("JSON".equalsIgnoreCase(clobTypeName)) {
                                ps.setString(i, resultSet.getString(i));
                            } else {
                                String clobValue = resultSet.getString(i);
                                if (clobValue == null) {
                                    ps.setNull(i, targetType);
                                } else {
                                    ps.setString(i, clobValue);
                                }
                            }
                            break;
                        case Types.BOOLEAN:
                        case Types.BIT:
                            boolean boolVal = resultSet.getBoolean(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setString(i, boolVal ? "1" : "0");
                            }
                            break;
                        case Types.SQLXML:
                            SQLXML sqlxmlData = resultSet.getSQLXML(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setString(i, sqlxmlData.getString());
                            }
                            break;
                        case Types.ARRAY:
                            Array arrayData = resultSet.getArray(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, targetType);
                            } else {
                                ps.setArray(i, arrayData);
                            }
                            break;
                        case Types.OTHER:
                            String typeName = rsmd.getColumnTypeName(i);
                            if ("DECFLOAT".equalsIgnoreCase(typeName)) {
                                java.math.BigDecimal decVal = resultSet.getBigDecimal(i);
                                if (resultSet.wasNull()) {
                                    ps.setNull(i, targetType);
                                } else {
                                    ps.setBigDecimal(i, decVal);
                                }
                            } else {
                                Object objVal = resultSet.getObject(i);
                                if (resultSet.wasNull()) {
                                    ps.setNull(i, targetType);
                                } else {
                                    ps.setObject(i, objVal);
                                }
                            }
                            break;
                        default:
                            ps.setString(i, resultSet.getString(i));
                            break;
                    }
                }

                ps.addBatch();

                if (++count % batchSize == 0) {
                    try {
                        ps.executeBatch();
                        this.getConnection().commit();
                    } catch (final SQLException batchException) {
                        logBatchException(batchException);
                        throw batchException;
                    }
                }
                totalRows++;
            } while (resultSet.next());
        }

        try {
            ps.executeBatch();
        } catch (final SQLException batchException) {
            logBatchException(batchException);
            throw batchException;
        }
        ps.close();

        this.getConnection().commit();
        return totalRows;
    }

    private Map<String, Integer> getSinkColumnTypes(String tableName) throws SQLException {
        Map<String, Integer> columnTypes = new HashMap<>();
        String schemaName = null;
        String resolvedTableName = tableName;

        if (resolvedTableName != null && resolvedTableName.contains(".")) {
            String[] parts = resolvedTableName.split("\\.", 2);
            schemaName = parts[0].replace("\"", "");
            resolvedTableName = parts[1];
        }

        if (resolvedTableName != null) {
            resolvedTableName = resolvedTableName.replace("\"", "");
        }

        if (schemaName != null) {
            schemaName = schemaName.toUpperCase();
        }
        if (resolvedTableName != null) {
            resolvedTableName = resolvedTableName.toUpperCase();
        }

        ResultSet columns = this.getConnection().getMetaData()
            .getColumns(null, schemaName, resolvedTableName, null);
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            int dataType = columns.getInt("DATA_TYPE");
            if (columnName != null) {
                columnTypes.put(columnName.toUpperCase(), dataType);
            }
        }
        columns.close();
        return columnTypes;
    }

    private int getSinkColumnType(Map<String, Integer> sinkColumnTypes, String columnLabel, int fallbackType) {
        if (columnLabel == null) {
            return fallbackType;
        }
        String normalized = columnLabel.replace("\"", "").toUpperCase();
        Integer type = sinkColumnTypes.get(normalized);
        return type != null ? type : fallbackType;
    }

    private void logBatchException(SQLException exception) {
        LOG.error("DB2 batch execution failed: {} (SQLState={}, errorCode={})",
            exception.getMessage(), exception.getSQLState(), exception.getErrorCode());
        SQLException next = exception.getNextException();
        int index = 1;
        while (next != null) {
            LOG.error("DB2 batch next exception #{}: {} (SQLState={}, errorCode={})",
                index, next.getMessage(), next.getSQLState(), next.getErrorCode());
            next = next.getNextException();
            index++;
        }
    }

    /**
     * Truncates the sink table using DELETE FROM to avoid DB2 TRUNCATE restrictions.
     *
     * @throws SQLException if the delete fails.
     */
    @Override
    protected void truncateTable() throws SQLException {
        super.truncateTable("DELETE FROM ");
    }

    /**
     * Creates the DB2 staging table using CREATE TABLE AS ... WITH NO DATA.
     *
     * @throws SQLException if creation fails.
     */
    @Override
    protected void createStagingTable() throws SQLException {
        Statement statement = this.getConnection().createStatement();
        String sinkStagingTable = getQualifiedStagingTableName();

        String allSinkColumns;
        if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
            allSinkColumns = this.options.getSinkColumns();
        } else if (this.options.getSourceColumns() != null && !this.options.getSourceColumns().isEmpty()) {
            allSinkColumns = this.options.getSourceColumns();
        } else {
            allSinkColumns = "*";
        }

        String sql = "CREATE TABLE " + sinkStagingTable + " AS (SELECT " + allSinkColumns
            + " FROM " + this.getSinkTableName() + ") WITH NO DATA";

        try {
            LOG.info("Creating staging table with this command: {}", sql);
            statement.executeUpdate(sql);
            statement.close();
            this.getConnection().commit();
        } catch (Exception e) {
            statement.close();
            this.connection.rollback();
            throw e;
        }
    }

    /**
     * Merges staging data into the sink table using DB2 MERGE.
     *
     * @throws SQLException if merge fails.
     */
    @Override
    protected void mergeStagingTable() throws SQLException {
        this.getConnection().commit();

        Statement statement = this.getConnection().createStatement();

        try {
            String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
            if (pks == null || pks.length == 0) {
                throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
            }

            String allColls = getAllSinkColumns(null);

            StringBuilder sql = new StringBuilder();
            sql.append("MERGE INTO ")
                .append(this.getSinkTableName())
                .append(" TRG USING (SELECT ")
                .append(allColls)
                .append(" FROM ")
                .append(getQualifiedStagingTableName())
                .append(" ) SRC ON ")
                .append(" (");

            for (int i = 0; i <= pks.length - 1; i++) {
                if (i >= 1) sql.append(" AND ");
                sql.append("SRC.").append(pks[i]).append("= TRG.").append(pks[i]);
            }

            sql.append(" ) WHEN MATCHED THEN UPDATE SET ");

            boolean hasUpdates = false;
            for (String colName : allColls.split("\\s*,\\s*")) {
                boolean contains = Arrays.asList(pks).contains(colName);
                boolean containsUppercase = Arrays.asList(pks).contains(colName.toUpperCase());
                boolean containsQuoted = Arrays.asList(pks).contains("\"" + colName.toUpperCase() + "\"");
                if (!contains && !containsUppercase && !containsQuoted) {
                    sql.append(" TRG.").append(colName).append(" = SRC.").append(colName).append(" ,");
                    hasUpdates = true;
                }
            }

            if (hasUpdates) {
                sql.setLength(sql.length() - 1);
            }

            sql.append(" WHEN NOT MATCHED THEN INSERT ( ").append(allColls)
                .append(" ) VALUES (");

            for (String colName : allColls.split("\\s*,\\s*")) {
                sql.append(" SRC.").append(colName).append(" ,");
            }

            sql.setLength(sql.length() - 1);
            sql.append(" ) ");

            LOG.info("Merging staging table and sink table with this command: {}", sql);
            statement.executeUpdate(sql.toString());
            statement.close();
            this.getConnection().commit();
        } catch (Exception e) {
            statement.close();
            this.connection.rollback();
            throw e;
        }
    }

    /**
     * DB2-specific pre-source tasks. No-op for DB2.
     *
     * @throws Exception if an unexpected error occurs.
     */
    @Override
    public void preSourceTasks() throws Exception {
        // Not necessary for DB2.
    }

    /**
     * DB2-specific post-source tasks. No-op for DB2.
     *
     * @throws Exception if an unexpected error occurs.
     */
    @Override
    public void postSourceTasks() throws Exception {
        // Not necessary for DB2.
    }

    private String getInsertSQLCommand(String tableName, String allColumns, int columnsNumber) {
        StringBuilder sqlCmd = new StringBuilder();

        sqlCmd.append("INSERT INTO ");
        sqlCmd.append(tableName);

        if (allColumns != null) {
            sqlCmd.append(" (");
            sqlCmd.append(allColumns);
            sqlCmd.append(")");
        }

        sqlCmd.append(" VALUES ( ");
        for (int i = 0; i <= columnsNumber - 1; i++) {
            if (i > 0) sqlCmd.append(",");
            sqlCmd.append("?");
        }
        sqlCmd.append(" )");

        return sqlCmd.toString();
    }
}
