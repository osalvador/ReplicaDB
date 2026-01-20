package org.replicadb.manager;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.*;
import java.util.Arrays;


public class OracleManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(OracleManager.class.getName());

    public OracleManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.ORACLE.getDriverClass();
    }


    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        String sqlCmd;

        // Read table with source-query option specified
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            oracleAlterSession(false);
            if (options.getJobs() == 1)
                sqlCmd = "SELECT  * FROM (" +
                        options.getSourceQuery() + ") where 0 = ?";
            else
                throw new UnsupportedOperationException("ReplicaDB on Oracle still not support custom query parallel process. Use properties instead: source.table, source.columns and source.where ");
        }
        // Read table with source-where option specified
        else if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            oracleAlterSession(false);
            sqlCmd = "SELECT  " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName) + " where " + options.getSourceWhere();
            if (options.getJobs() == 1)
                sqlCmd = sqlCmd + " AND 0 = ?";
            else
                sqlCmd = sqlCmd + " AND ora_hash(rowid," + (options.getJobs() - 1) + ") = ?";
        } else {
            // Full table read. NO_IDEX and Oracle direct Read
            oracleAlterSession(true);
            sqlCmd = "SELECT /*+ NO_INDEX(" + escapeTableName(tableName) + ")*/ " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            if (options.getJobs() == 1)
                sqlCmd = sqlCmd + " where 0 = ?";
            else
                sqlCmd = sqlCmd + " where ora_hash(rowid," + (options.getJobs() - 1) + ") = ?";


        }

        return super.execute(sqlCmd, (Object) nThread);
    }

    public void oracleAlterSession(Boolean directRead) throws SQLException {
        // Specific Oracle Alter sessions for reading data
        Statement stmt = this.getConnection().createStatement();
        stmt.executeUpdate("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
        stmt.executeUpdate("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS' ");
        stmt.executeUpdate("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF' ");
        stmt.executeUpdate("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZH:TZM' ");
        stmt.executeUpdate("ALTER SESSION ENABLE PARALLEL DML");

        // The recyclebin is available since version 10
        DatabaseMetaData meta = this.getConnection().getMetaData();
        if ( meta.getDatabaseMajorVersion() >= 10 ) stmt.executeUpdate("ALTER SESSION SET recyclebin = OFF");

        if (directRead) stmt.executeUpdate("ALTER SESSION SET \"_serial_direct_read\"=true ");

        stmt.close();
    }


    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException {

        int totalRows = 0;
        ResultSetMetaData rsmd = resultSet.getMetaData();
        String tableName;

        // Get table name and columns
        if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
            tableName = getSinkTableName();
        } else {
            tableName = getQualifiedStagingTableName();
        }

        String allColumns = getAllSinkColumns(rsmd);
        int columnsNumber = rsmd.getColumnCount();

        String sqlCdm = getInsertSQLCommand(tableName, allColumns, columnsNumber);
        PreparedStatement ps = this.getConnection().prepareStatement(sqlCdm);

        final int batchSize = options.getFetchSize();
        int count = 0;

        LOG.info("Inserting data with this command: {}",sqlCdm);

        oracleAlterSession(true);

        if (resultSet.next()) {
            // Create Bandwidth Throttling
            BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);

            do {
                bt.acquiere();

                // Get Columns values
                for (int i = 1; i <= columnsNumber; i++) {

                    switch (rsmd.getColumnType(i)) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.LONGVARCHAR:
                            ps.setString(i, resultSet.getString(i));
                            break;
                        case Types.INTEGER:
                        case Types.TINYINT:
                        case Types.SMALLINT:
                            ps.setInt(i, resultSet.getInt(i));
                            break;
                        case Types.BIGINT:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            ps.setBigDecimal(i, resultSet.getBigDecimal(i));
                            break;
                        case Types.DOUBLE:
                            ps.setDouble(i, resultSet.getDouble(i));
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                            ps.setFloat(i, resultSet.getFloat(i));
                            break;
                        case Types.DATE:
                            ps.setDate(i, resultSet.getDate(i));
                            break;
                        case Types.TIMESTAMP:
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        case -101:
                        case -102:
                            ps.setTimestamp(i, resultSet.getTimestamp(i));
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            ps.setBytes(i, resultSet.getBytes(i));
                            break;
                        case Types.BLOB:
                            Blob blobData = getBlob(resultSet,i);
                            ps.setBlob(i, blobData);
                            if (blobData != null) blobData.free();
                            break;
                        case Types.CLOB:
                            Clob clobData = resultSet.getClob(i);
                            if (clobData != null) {
                                long clobLength = clobData.length();
                                if (clobLength > 0) {
                                    ps.setCharacterStream(i, clobData.getCharacterStream(), clobLength);
                                } else {
                                    ps.setNull(i, Types.CLOB);
                                }
                                clobData.free();
                            } else {
                                ps.setNull(i, Types.CLOB);
                            }
                            break;
                        case Types.BOOLEAN:
                        case Types.BIT:
                            // Oracle doesn't have native BOOLEAN, convert to string "true"/"false"
                            Boolean boolValue = resultSet.getBoolean(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, Types.VARCHAR);
                            } else {
                                ps.setString(i, boolValue.toString());
                            }
                            break;
                        case Types.NVARCHAR:
                        case Types.NCHAR:
                        case Types.LONGNVARCHAR:
                            ps.setNString(i, resultSet.getNString(i));
                            break;
                        case Types.SQLXML:
                            SQLXML sqlXmlData = resultSet.getSQLXML(i);
                            if (!resultSet.wasNull()){
                                SQLXML sinkXmlData = this.getConnection().createSQLXML();
                                IOUtils.copy(sqlXmlData.getBinaryStream(), sinkXmlData.setBinaryStream());
                                ps.setSQLXML(i, sinkXmlData);
                                sqlXmlData.free();
                                sinkXmlData.free();
                            } else {
                                ps.setSQLXML(i, sqlXmlData);
                            }
                            break;
                        case Types.ROWID:
                            ps.setRowId(i, resultSet.getRowId(i));
                            break;
                        case Types.ARRAY:
                            // Convert array to string representation for Oracle
                            java.sql.Array arrayData = resultSet.getArray(i);
                            if (arrayData != null) {
                                ps.setString(i, arrayData.toString());
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        case Types.STRUCT:
                            ps.setObject(i, resultSet.getObject(i), Types.STRUCT);
                            break;
                        case Types.OTHER:
                            // PostgreSQL specific types (bytea, json, xml, etc.) - convert to string
                            Object otherData = resultSet.getObject(i);
                            if (otherData != null) {
                                if (otherData instanceof byte[]) {
                                    ps.setBytes(i, (byte[]) otherData);
                                } else {
                                    ps.setString(i, otherData.toString());
                                }
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        case Types.TIME:
                        case Types.TIME_WITH_TIMEZONE:
                            // Oracle doesn't have TIME type, store as string
                            java.sql.Time timeData = resultSet.getTime(i);
                            if (timeData != null) {
                                ps.setString(i, timeData.toString());
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        // Oracle INTERVAL types (not in standard java.sql.Types)
                        case -104: // INTERVALDS (INTERVAL DAY TO SECOND)
                        case -103: // INTERVALYM (INTERVAL YEAR TO MONTH)
                            Object intervalData = resultSet.getObject(i);
                            if (intervalData != null) {
                                ps.setObject(i, intervalData);
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        default:
                            // Fallback: convert to string to avoid ORA-17004
                            Object defaultData = resultSet.getObject(i);
                            if (defaultData != null) {
                                if (defaultData instanceof byte[]) {
                                    ps.setBytes(i, (byte[]) defaultData);
                                } else if (defaultData instanceof Number) {
                                    ps.setBigDecimal(i, new java.math.BigDecimal(defaultData.toString()));
                                } else {
                                    ps.setString(i, defaultData.toString());
                                }
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            LOG.debug("Column {} has unhandled JDBC type {}, converted to string", i, rsmd.getColumnType(i));
                            break;
                    }
                }

                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                    this.getConnection().commit();
                }

                totalRows++;
            } while (resultSet.next());
        }

        ps.executeBatch(); // insert remaining records
        ps.close();

        this.getConnection().commit();
        return totalRows;
    }

    private String getInsertSQLCommand(String tableName, String allColumns, int columnsNumber) {

        StringBuilder sqlCmd = new StringBuilder();

        //sqlCmd.append("INSERT INTO /*+APPEND_VALUES*/ ");
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

    @Override
    protected void createStagingTable() throws SQLException {

        Statement statement = this.getConnection().createStatement();
        String sinkStagingTable = getQualifiedStagingTableName();

        // Get sink columns.
        String allSinkColumns = null;
        if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
            allSinkColumns = this.options.getSinkColumns();
        } else if (this.options.getSourceColumns() != null && !this.options.getSourceColumns().isEmpty()) {
            allSinkColumns = this.options.getSourceColumns();
        } else {
            allSinkColumns = "*";
        }

        String sql = " CREATE TABLE " + sinkStagingTable + " NOLOGGING AS (SELECT " + allSinkColumns + " FROM " + this.getSinkTableName() + " WHERE rownum = -1 ) ";

        LOG.info("Creating staging table with this command: {}", sql);
        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();

    }

    @Override
    protected void mergeStagingTable() throws SQLException {
        this.getConnection().commit();

        Statement statement = this.getConnection().createStatement();

        String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
        // Primary key is required
        if (pks == null || pks.length == 0) {
            throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
        }

        // options.sinkColumns was set during the insertDataToTable
        String allColls = getAllSinkColumns(null);
        // Oracle use columns uppercase
        //allColls = allColls.toUpperCase();

        StringBuilder sql = new StringBuilder();
        sql.append("MERGE /*+ PARALLEL */ INTO ")
                .append(this.getSinkTableName())
                .append(" trg USING (SELECT ")
                .append(allColls)
                .append(" FROM ")
                .append(getQualifiedStagingTableName())
                .append(" ) src ON ")
                .append(" (");

        for (int i = 0; i <= pks.length - 1; i++) {
            if (i >= 1) sql.append(" AND ");
            sql.append("src.").append(pks[i]).append("= trg.").append(pks[i]);
        }

        sql.append(" ) WHEN MATCHED THEN UPDATE SET ");

        // Set all columns for UPDATE SET statement
        for (String colName : allColls.split("\\s*,\\s*")) {

            boolean contains = Arrays.asList(pks).contains(colName);
            boolean containsUppercase = Arrays.asList(pks).contains(colName.toUpperCase());
            boolean containsQuoted = Arrays.asList(pks).contains("\""+colName.toUpperCase()+"\"");
            if (!contains && !containsUppercase && !containsQuoted)
                sql.append(" trg.").append(colName).append(" = src.").append(colName).append(" ,");
        }
        // Delete the last comma
        sql.setLength(sql.length() - 1);


        sql.append(" WHEN NOT MATCHED THEN INSERT ( ").append(allColls).
                append(" ) VALUES (");

        // all columns for INSERT VALUES statement
        for (String colName : allColls.split("\\s*,\\s*")) {
            sql.append(" src.").append(colName).append(" ,");
        }
        // Delete the last comma
        sql.setLength(sql.length() - 1);

        sql.append(" ) ");

        LOG.info("Merging staging table and sink table with this command: {}", sql);
        statement.executeUpdate(sql.toString());
        statement.close();
        this.getConnection().commit();
    }

    @Override
    public void preSourceTasks() {}

    @Override
    public void postSourceTasks() {}

    @Override
    public void dropStagingTable() throws SQLException {
        Statement stmt = null;
        try {
            // Commit any pending transaction before ALTER SESSION
            if (!this.getConnection().getAutoCommit()) {
                this.getConnection().commit();
            }
            // Disable Oracle RECYCLEBIN for DROP operation
            stmt = this.getConnection().createStatement();
            DatabaseMetaData meta = this.getConnection().getMetaData();
            if (meta.getDatabaseMajorVersion() >= 10) {
                stmt.executeUpdate("ALTER SESSION SET recyclebin = OFF");
            }
        } catch (SQLException e) {
            LOG.warn("Could not disable recyclebin: {}", e.getMessage());
        } finally {
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException ignored) {}
            }
        }
        super.dropStagingTable();
    }
}
