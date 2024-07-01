package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import microsoft.sql.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.rowset.CsvCachedRowSetImpl;
import org.replicadb.rowset.MongoDBRowSetImpl;

import javax.sql.RowSet;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
      copyOptions.setBatchSize(options.getFetchSize());
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
      try {
         // Write from the source to the destination.
         // If the source ResulSet is an implementation of RowSet (e.g. csv file) cast it.
         if (resultSet instanceof RowSet) {
            bulkCopy.writeToServer((RowSet) resultSet);
         } else {
            bulkCopy.writeToServer(resultSet);
         }


      } catch (SQLException ex) {
         LOG.error("Error while performing BulkCopy into {} ", tableName, ex);
         // log the error
         logColumnLengthError(bulkCopy, ex);

         throw ex;
      }

      bulkCopy.close();

      // TODO: getAllSinkColumns should not update the sinkColumns property. Change it in Oracle and check it in Postgres
      // Set Sink columns
      getAllSinkColumns(rsmd);
      
      this.getConnection().commit();
      return getNumFetchedRows(resultSet);
   }

   /**
    * Get the number of rows fetched from the ResultSet using reflection.
    *
    * @param resultSet the ResultSet
    * @return the number of rows fetched
    */
   private static int getNumFetchedRows(ResultSet resultSet) {
      int numFetchedRows = 0;
      Field fi = null;

      if (resultSet instanceof SQLServerResultSet) {
         try {
            fi = SQLServerResultSet.class.getDeclaredField("rowCount");
            fi.setAccessible(true);
            numFetchedRows = (int) fi.get(resultSet);
         } catch (Exception e) {
            // ignore exception
            numFetchedRows = 0;
         }
      } else if (resultSet instanceof CsvCachedRowSetImpl) {
         numFetchedRows = ((CsvCachedRowSetImpl) resultSet).getRowCount();         
      } else if (resultSet instanceof MongoDBRowSetImpl) {         
         numFetchedRows = ((MongoDBRowSetImpl) resultSet).getRowCount();
      }

      return numFetchedRows;
   }

   /**
    * Log the error message and the column name and length that caused the error using reflection.
    *
    * @param bulkCopy the SQLServerBulkCopy object
    * @param ex       the SQLException
    */
   private static void logColumnLengthError (SQLServerBulkCopy bulkCopy, SQLException ex) {
      if (ex.getMessage().contains("Received an invalid column length from the bcp client for colid")) {
         try {
            String pattern = "\\d+";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(ex.getMessage());
            if (m.find()) {
               // get source column metadata
               Field fi = SQLServerBulkCopy.class.getDeclaredField("srcColumnMetadata");
               fi.setAccessible(true);
               HashMap<Integer, Object> srcColumnsMetadata = (HashMap<Integer, Object>) fi.get(bulkCopy);
               // get destination column metadata
               fi = SQLServerBulkCopy.class.getDeclaredField("destColumnMetadata");
               fi.setAccessible(true);
               HashMap<Integer, Object> destColumnsMetadata = (HashMap<Integer, Object>) fi.get(bulkCopy);

               // iterate over the HashMap and log the columns metadata and mapping
               for (Integer key : destColumnsMetadata.keySet()) {

                  // get source column metadata
                  Object srcColumnMetadata = srcColumnsMetadata.get(key);
                  // get source column name
                  fi = srcColumnMetadata.getClass().getDeclaredField("columnName");
                  fi.setAccessible(true);
                  String srcColumnName = (String) fi.get(srcColumnMetadata);
                  // get source column precision
                  fi = srcColumnMetadata.getClass().getDeclaredField("precision");
                  fi.setAccessible(true);
                  int srcColumnPrecision = (int) fi.get(srcColumnMetadata);
                  // get source column scale
                  fi = srcColumnMetadata.getClass().getDeclaredField("scale");
                  fi.setAccessible(true);
                  int srcColumnScale = (int) fi.get(srcColumnMetadata);
                  // get source column type
                  fi = srcColumnMetadata.getClass().getDeclaredField("jdbcType");
                  fi.setAccessible(true);
                  int srcColumnType = (int) fi.get(srcColumnMetadata);
                  String srcType = getJdbcTypeName(srcColumnType);


                  // get destination column metadata
                  Object destColumnMeta = destColumnsMetadata.get(key);
                  // get the column Name
                  fi = destColumnMeta.getClass().getDeclaredField("columnName");
                  fi.setAccessible(true);
                  String destColumnName = (String) fi.get(destColumnMeta);
                  // get the precision
                  fi = destColumnMeta.getClass().getDeclaredField("precision");
                  fi.setAccessible(true);
                  int destPrecision = (int) fi.get(destColumnMeta);
                  // get the scale
                  fi = destColumnMeta.getClass().getDeclaredField("scale");
                  fi.setAccessible(true);
                  int destScale = (int) fi.get(destColumnMeta);
                  // get the type
                  fi =  destColumnMeta.getClass().getDeclaredField("jdbcType");
                  fi.setAccessible(true);
                  int destJdbcType = (int) fi.get(destColumnMeta);
                  // convert to string the type
                  String destType = "";
                  destType = getJdbcTypeName(destJdbcType);

                  // Log source column mapped to sink column with metadata
                  LOG.debug("colid {} : Source column {} ({}:(precision:{},scale:{})) mapped to sink column {} ({}:(precision:{},scale:{}))", key, srcColumnName, srcType, srcColumnPrecision, srcColumnScale, destColumnName, destType, destPrecision, destScale);

               }
            }
         } catch (NoSuchFieldException | IllegalAccessException e) {
            // ignore exception
         }
      }
   }

   private static String getJdbcTypeName (int destJdbcType) {
      String destType = "";
      try {
         destType = JDBCType.valueOf(destJdbcType).getName();
      } catch (IllegalArgumentException e) {
         // try to get the name of the type from Types class based on its value
         Field[] fields = Types.class.getFields();
         for (Field field : fields) {
            if (field.getType().equals(int.class)) {
               try {
                  if (field.getInt(null) == destJdbcType) {
                     destType = field.getName();
                     break;
                  }
               } catch (IllegalAccessException e1) {
                  LOG.error("Error while getting the name of the type from Types class based on its value", e1);
               }
            }
         }

      }
      return destType;
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
            sqlCmd = sqlCmd + " where " + options.getSourceWhere() + " AND ABS(CHECKSUM(%% physloc %%)) % " + (options.getJobs()) + " = ?";

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
