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
                            ps.setBytes(i,resultSet.getBytes(i));
                            break;
                        case Types.BLOB:
                            Blob blobData = getBlob(resultSet,i);
                            ps.setBlob(i, blobData);
                            if (blobData != null) blobData.free();
                            break;
                        case Types.CLOB:
                            Clob clobData = resultSet.getClob(i);
                            //ps.setClob(i, clobData);
                            if (clobData != null) 
                            {                            	
                            	//Se establece los datos del campo CLOB como un objeto java.io.Reader
                            	ps.setClob(i, clobData.getCharacterStream());
                            	clobData.free();
                            }
                            break;
                        case Types.BOOLEAN:
                            ps.setBoolean(i, resultSet.getBoolean(i));
                            break;
                        case Types.NVARCHAR:
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
                        //case Types.ARRAY: // Not supported by Oracle
                        case Types.STRUCT:
                            ps.setObject(i, resultSet.getObject(i),Types.STRUCT);
                            break;
                        default:
                            ps.setString(i, resultSet.getString(i));
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
        // Disable Oracle RECYCLEBIN
        oracleAlterSession(false);
        super.dropStagingTable();
    }
}
