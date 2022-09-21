package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.sql.*;


public class StandardJDBCManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(StandardJDBCManager.class.getName());

   public StandardJDBCManager (ToolOptions opts, DataSourceType dsType) {
      super(opts);
      this.dsType = dsType;
   }

   @Override
   public String getDriverClass () {

      // Only complete mode is available
      if (!options.getMode().equals(ReplicationMode.COMPLETE.getModeText())){
         throw new UnsupportedOperationException("Only the 'complete' mode is supported by Standard JDBC Manager.");
      } else if (this.options.getJobs() > 1) {
         throw new UnsupportedOperationException("Parallel processing is not supported in the standard JDBC Manager, jobs=1 must be set.");
      }
      String driverClassName = "";
      if (this.dsType.equals(DataSourceType.SOURCE)) {
         driverClassName = this.options.getSourceConnectionParams().getProperty("driver");
      } else {
         driverClassName = this.options.getSinkConnectionParams().getProperty("driver");
      }

      // if driverClassName is null or empty throw exception
      if (driverClassName == null || driverClassName.isEmpty()) {
         throw new IllegalArgumentException("Driver class name is not defined in \'[source,sink].connect.parameter.driver\'");
      }
      return driverClassName;
   }


   @Override
   public ResultSet readTable (String tableName, String[] columns, int nThread) throws SQLException {

      // If table name parameter is null get it from options
      tableName = tableName == null ? this.options.getSourceTable() : tableName;

      // If columns parameter is null, get it from options
      String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

      String sqlCmd;

      // Read table with source-query option specified
      if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
         sqlCmd = "SELECT  * FROM (" +
             options.getSourceQuery() + ") as T1 ";
      } else {

         sqlCmd = "SELECT " +
             allColumns +
             " FROM " +
             escapeTableName(tableName);

         // Source Where
         if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
         }

      }

      return super.execute(sqlCmd);

   }

   @Override
   public int insertDataToTable (ResultSet resultSet, int taskId) throws SQLException, IOException {
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

      LOG.info("Inserting data with this command: {}", sqlCdm);

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
                     ps.setBytes(i, resultSet.getBytes(i));
                     break;
                  case Types.BLOB:
                     Blob blobData = resultSet.getBlob(i);
                     ps.setBlob(i, blobData);
                     if (blobData != null) blobData.free();
                     break;
                  case Types.CLOB:
                     Clob clobData = resultSet.getClob(i);
                     ps.setClob(i, clobData);
                     if (clobData != null) clobData.free();
                     break;
                  case Types.BOOLEAN:
                     ps.setBoolean(i, resultSet.getBoolean(i));
                     break;
                  case Types.NVARCHAR:
                     ps.setNString(i, resultSet.getNString(i));
                     break;
                  case Types.SQLXML:
                     SQLXML sqlxmlData = resultSet.getSQLXML(i);
                     ps.setSQLXML(i, sqlxmlData);
                     if (sqlxmlData != null) sqlxmlData.free();
                     break;
                  case Types.ROWID:
                     ps.setRowId(i, resultSet.getRowId(i));
                     break;
                  case Types.ARRAY:
                     Array arrayData = resultSet.getArray(i);
                     ps.setArray(i, arrayData);
                     arrayData.free();
                     break;
                  case Types.STRUCT:
                     ps.setObject(i, resultSet.getObject(i), Types.STRUCT);
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

   private String getInsertSQLCommand (String tableName, String allColumns, int columnsNumber) {

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

   @Override
   protected void createStagingTable () throws SQLException {
      // Not necessary
   }
   @Override
   public void postSourceTasks () throws Exception {
      // Not necessary
   }
   @Override
   public void preSourceTasks () throws Exception {
      // Not necessary
   }
   @Override
   protected void mergeStagingTable () throws Exception {
      // Not necessary
   }

   @Override
   protected void truncateTable () throws SQLException {
      super.truncateTable("DELETE ");
   }
}
