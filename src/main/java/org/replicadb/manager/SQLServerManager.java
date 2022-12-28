package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import javax.sql.RowSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;

public class SQLServerManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(SQLServerManager.class.getName());

   /**
    * Constructs the SqlManager.
    *
    * @param opts the ReplicaDB ToolOptions describing the user's requested action.
    */
   public SQLServerManager (ToolOptions opts, DataSourceType dsType) {
      super(opts);
      this.dsType = dsType;
   }

   @Override
   public String getDriverClass () {
      return JdbcDrivers.SQLSERVER.getDriverClass();
   }

   private void setIdentityInsert (String tableName, Boolean isSetIdentityOn) throws SQLException {
      String valueToSetIdentity;
      if (Boolean.TRUE.equals(isSetIdentityOn)) valueToSetIdentity = "ON";
      else valueToSetIdentity = "OFF";

      Statement stmt = this.getConnection().createStatement();
      String sqlCommand = "IF OBJECTPROPERTY(OBJECT_ID('" +
          tableName +
          "'), 'TableHasIdentity') = 1 " +
          "SET IDENTITY_INSERT " +
          tableName +
          " " +
          valueToSetIdentity;
      LOG.info(sqlCommand);
      stmt.executeUpdate(sqlCommand);
      stmt.close();
   }

   @Override
   public int insertDataToTable (ResultSet resultSet, int taskId) throws SQLException {

      String tableName;

      // Get table name and columns
      if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
         tableName = getSinkTableName();
      } else {
         tableName = getQualifiedStagingTableName();
      }

      ResultSetMetaData rsmd = resultSet.getMetaData();
      int columnCount = rsmd.getColumnCount();

      SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(this.getConnection());
      // BulkCopy Options
      SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
      copyOptions.setBulkCopyTimeout(0);
      bulkCopy.setBulkCopyOptions(copyOptions);

      bulkCopy.setDestinationTableName(tableName);

      // Columns Mapping
      if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
         String sinkColumns = getAllSinkColumns(rsmd);
         // Remove quotes from column names, which are not supported by SQLServerBulkCopy
         String[] sinkColumnsArray = sinkColumns.replace("\"", "").split(",");
         LOG.trace("Mapping columns: source --> sink");
         for (int i = 1; i <= sinkColumnsArray.length; i++) {
            bulkCopy.addColumnMapping(rsmd.getColumnName(i), sinkColumnsArray[i - 1]);
            LOG.trace("{} --> {}", rsmd.getColumnName(i), sinkColumnsArray[i - 1]);
         }
      } else {
         for (int i = 1; i <= columnCount; i++) {
            LOG.trace("source {} - {} sink", rsmd.getColumnName(i), i);
            bulkCopy.addColumnMapping(rsmd.getColumnName(i), i);
         }
      }

      LOG.debug("Performing BulkCopy into {} ", tableName);
      // Write from the source to the destination.
      // If the source ResulSet is an implementation of RowSet (e.g. csv file) cast it.
      if (resultSet instanceof RowSet) {
         bulkCopy.writeToServer((RowSet) resultSet);
      } else {
         bulkCopy.writeToServer(resultSet);
      }

      bulkCopy.close();

      // TODO: getAllSinkColumns should not update the sinkColumns property. Change it in Oracle and check it in Postgres
      // Set Sink columns
      getAllSinkColumns(rsmd);

      this.getConnection().commit();
      // Return the last row number
      return resultSet.getRow();

   }

   @Override
   protected void createStagingTable () throws SQLException {
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

      String sql = " SELECT " + allSinkColumns + " INTO " + sinkStagingTable + " FROM " + this.getSinkTableName() + " WHERE 0 = 1 ";

      LOG.info("Creating staging table with this command: {}", sql);
      statement.executeUpdate(sql);
      statement.close();
      this.getConnection().commit();
   }

   @Override
   protected void mergeStagingTable () throws SQLException {
      this.getConnection().commit();

      Statement statement = this.getConnection().createStatement();

      String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
      // Primary key is required
      if (pks == null || pks.length == 0) {
         throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
      }

      // options.sinkColumns was set during the insertDataToTable
      String allColls = getAllSinkColumns(null);

      StringBuilder sql = new StringBuilder();
      sql.append("MERGE INTO ")
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

       sql.append(" ) ");
       LOG.trace("allColls: {} \n pks: {}", allColls, pks);

       // Set all columns for UPDATE SET statement
       String allColSelect = Arrays.stream(allColls.split("\\s*,\\s*"))
           .filter(colName -> {
               boolean contains = Arrays.asList(pks).contains(colName);
               boolean containsQuoted = Arrays.asList(pks).contains("\"" + colName + "\"");
               return !contains && !containsQuoted;
           }).map(colName -> {
               return String.format("trg.%s = src.%s", colName, colName);
           }).collect(Collectors.joining(", "));

      if (allColSelect.length() > 0) {
         sql.append(" WHEN MATCHED THEN UPDATE SET ");
         sql.append(allColSelect);
      } else {
         LOG.warn("All columns in the sink table are Primary Keys. WHEN MATCHED DO NOTHING.");
      }

      sql.append(" WHEN NOT MATCHED THEN INSERT ( ").append(allColls).
          append(" ) VALUES (");

      // all columns for INSERT VALUES statement
      for (String colName : allColls.split("\\s*,\\s*")) {
         sql.append(" src.").append(colName).append(" ,");
      }
      // Delete the last comma
      sql.setLength(sql.length() - 1);

      sql.append(" ); ");

      LOG.info("Merging staging table and sink table with this command: {}", sql);
      statement.executeUpdate(sql.toString());
      statement.close();
      this.getConnection().commit();
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

         if (options.getJobs() == 1)
            sqlCmd = "SELECT * FROM (" +
                options.getSourceQuery() + ") as REPDBQUERY where 0 = ?";
         else
            throw new UnsupportedOperationException("ReplicaDB on SQLServer still not support custom query parallel process. Use properties instead: source.table, source.columns and source.where ");
      }
      // Read table with source-where option specified
      else if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {

         sqlCmd = "SELECT  " +
             allColumns +
             " FROM " +
             escapeTableName(tableName);

         if (options.getJobs() == 1)
            sqlCmd = sqlCmd + " where " + options.getSourceWhere() + " AND 0 = ?";
         else
            sqlCmd = sqlCmd + " where " + options.getSourceWhere() + " ABS(CHECKSUM(%% physloc %%)) % " + (options.getJobs()) + " = ?";

      } else {
         // Full table read. Force NO_IDEX and table scan
         sqlCmd = "SELECT  " +
             allColumns +
             " FROM " +
             escapeTableName(tableName);

         if (options.getJobs() == 1)
            sqlCmd = sqlCmd + " where 0 = ?";
         else
            sqlCmd = sqlCmd + " where ABS(CHECKSUM(%% physloc %%)) % " + (options.getJobs()) + " = ?";

      }

      return super.execute(sqlCmd, (Object) nThread);

   }


   @Override
   public void preSourceTasks () {/*Not implemented*/}

   @Override
   public void postSourceTasks () {/*Not implemented*/}

   @Override
   public void postSinkTasks () throws Exception {
      setIdentityInsert(getSinkTableName(), true);
      super.postSinkTasks();
      setIdentityInsert(getSinkTableName(), false);
   }
}
