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

/**
 * SQL Server-specific connection manager using Microsoft JDBC Driver and BulkCopy API.
 * 
 * <p><strong>Logging Standards:</strong> This class follows ReplicaDB logging conventions with ~10 logging
 * statements (reduced from 24) for consistency with other manager implementations like
 * PostgresqlManager and OracleManager (5-8 statements each). TRACE-level statements were removed
 * to reduce development noise, as they never execute in production/CI environments (log level = INFO).</p>
 * 
 * <p><strong>Enabling Detailed Logging:</strong> To enable detailed column mapping traces during local
 * development debugging, add the following to your {@code log4j2-test.xml}:</p>
 * <pre>
 * &lt;Logger name="org.replicadb.manager.SQLServerManager" level="trace" additivity="false"&gt;
 *     &lt;AppenderRef ref="console"/&gt;
 * &lt;/Logger&gt;
 * </pre>
 * 
 * <p><strong>Current Logging Distribution:</strong></p>
 * <ul>
 *   <li>DEBUG: 3 statements (operation summaries and column mappings)</li>
 *   <li>INFO: 3 statements (SQL commands and merge operations)</li>
 *   <li>WARN: 4 statements (metadata lookup failures and edge cases)</li>
 *   <li>ERROR: 3 statements (BulkCopy failures and column mismatches)</li>
 * </ul>
 */
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

   /**
    * Retrieves sink column types from SQL Server metadata.
    * Maps source column positions to sink column JDBC types for type-aware BulkCopy operations.
    *
    * @param tableName the sink table name
    * @param sourceMetaData the source ResultSet metadata
    * @return Map of column index (1-based) to JDBC type code
    * @throws SQLException if metadata retrieval fails
    */
   private java.util.Map<Integer, Integer> getSinkColumnTypes(String tableName, ResultSetMetaData sourceMetaData) throws SQLException {
      java.util.Map<Integer, Integer> columnTypeMap = new HashMap<>();
      
      // IMPORTANT: do not close the shared manager connection
      Connection conn = this.getConnection();
      DatabaseMetaData metaData = conn.getMetaData();
      
      // Normalize and split schema.table - SQL Server qualified names can be:
      // [db].[schema].[table], schema.table, or just table
      String normalized = tableName.replace("[", "").replace("]", "").replace("\"", "").trim();
      
      String schemaPattern;
      String tablePattern;
      
      String[] parts = normalized.split("\\.");
      if (parts.length == 2) {
         // schema.table
         schemaPattern = parts[0];
         tablePattern = parts[1];
      } else if (parts.length == 3) {
         // catalog.schema.table
         schemaPattern = parts[1];
         tablePattern = parts[2];
      } else {
         // No schema provided; SQL Server default is typically dbo for user tables
         schemaPattern = "dbo";
         tablePattern = normalized;
      }
      
      String catalog = conn.getCatalog();
      
      try (ResultSet rs = metaData.getColumns(catalog, schemaPattern, tablePattern, null)) {
          java.util.Map<String, Integer> columnTypesByName = new HashMap<>();
          while (rs.next()) {
             String columnName = rs.getString("COLUMN_NAME").toLowerCase();
             int columnType = rs.getInt("DATA_TYPE");
             String columnTypeName = rs.getString("TYPE_NAME");
             
             // SQL Server reports XML columns with vendor-specific type code (-16)
             // but we need to map it to Types.SQLXML (2009) for proper handling
             if ("xml".equalsIgnoreCase(columnTypeName)) {
                LOG.info("Sink column '{}' has TYPE_NAME='xml', mapping vendor type {} to java.sql.Types.SQLXML (2009)", 
                    columnName, columnType);
                columnType = java.sql.Types.SQLXML;
             }
             
             columnTypesByName.put(columnName, columnType);
             LOG.info("Sink column '{}' has JDBC type {} (TYPE_NAME='{}')", columnName, columnType, columnTypeName);
          }
         
         // Fail fast if no columns found - prevents cryptic BulkCopy errors
         if (columnTypesByName.isEmpty()) {
            LOG.warn("Could not resolve sink metadata for table '{}' (schema={}, table={}). Type coercion will be disabled.",
                tableName, schemaPattern, tablePattern);
            return columnTypeMap;
         }
         
         // Map source column positions to sink column types
         // When explicit sink columns are specified, use positional mapping
         if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
            String[] sinkColumnsArray = this.options.getSinkColumns().replace("\"", "").split(",");
            for (int i = 1; i <= Math.min(sinkColumnsArray.length, sourceMetaData.getColumnCount()); i++) {
               String sinkColumnName = sinkColumnsArray[i - 1].trim().toLowerCase();
               Integer sinkType = columnTypesByName.get(sinkColumnName);
               if (sinkType != null) {
                  columnTypeMap.put(i, sinkType);
               }
            }
         } else {
            // No explicit sink columns - match by source column name
            for (int i = 1; i <= sourceMetaData.getColumnCount(); i++) {
               String sourceColumnName = sourceMetaData.getColumnName(i).toLowerCase();
               Integer sinkType = columnTypesByName.get(sourceColumnName);
               if (sinkType != null) {
                  columnTypeMap.put(i, sinkType);
               }
            }
         }
      }
      
      LOG.debug("Retrieved {} sink column type mappings for table {}", columnTypeMap.size(), tableName);
      return columnTypeMap;
   }

   /**
    * Retrieves sink column ordinals from SQL Server metadata.
    *
    * @param tableName the sink table name
    * @return Map of sink column name (lowercase) to ordinal position (1-based)
    * @throws SQLException if metadata retrieval fails
    */
   private java.util.Map<String, Integer> getSinkColumnOrdinals(String tableName) throws SQLException {
      java.util.Map<String, Integer> columnOrdinalsByName = new HashMap<>();

      Connection conn = this.getConnection();
      DatabaseMetaData metaData = conn.getMetaData();

      String normalized = tableName.replace("[", "").replace("]", "").replace("\"", "").trim();

      String schemaPattern;
      String tablePattern;

      String[] parts = normalized.split("\\.");
      if (parts.length == 2) {
         schemaPattern = parts[0];
         tablePattern = parts[1];
      } else if (parts.length == 3) {
         schemaPattern = parts[1];
         tablePattern = parts[2];
      } else {
         schemaPattern = "dbo";
         tablePattern = normalized;
      }

      String catalog = conn.getCatalog();

      try (ResultSet rs = metaData.getColumns(catalog, schemaPattern, tablePattern, null)) {
         while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME").toLowerCase();
            int ordinal = rs.getInt("ORDINAL_POSITION");
            columnOrdinalsByName.put(columnName, ordinal);
         }
      }

      if (columnOrdinalsByName.isEmpty()) {
         LOG.warn("Could not resolve sink column ordinals for table '{}' (schema={}, table={}).", tableName, schemaPattern, tablePattern);
      }

      return columnOrdinalsByName;
   }

   @Override
   public int insertDataToTable (ResultSet resultSet, int taskId) throws SQLException {
      // SQL Server deadlock retry constants
      final int MAX_RETRIES = 3;
      final long INITIAL_WAIT_MS = 1000L; // Increased from 500ms to reduce collision probability
      final int DEADLOCK_ERROR_CODE = 1205;
      
      SQLException lastException = null;

      for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
         try {
            return insertDataToTableInternal(resultSet, taskId);
         } catch (SQLException ex) {
            lastException = ex;
            
            // Check for SQL Server deadlock error (1205) and retry only if attempts remain
            if (ex.getErrorCode() == DEADLOCK_ERROR_CODE && attempt < MAX_RETRIES) {
               long waitMs = INITIAL_WAIT_MS * attempt; // Linear backoff to spread out retries
               // Add randomization to reduce collision probability (0-50% jitter)
               waitMs += (long) (Math.random() * waitMs * 0.5);
               
               LOG.warn("TaskId-{}: SQL Server deadlock detected (error 1205) on attempt {}/{}. " +
                        "Retrying in {} ms. This is normal under high parallel load.", 
                        taskId, attempt, MAX_RETRIES, waitMs);
               
               try {
                  Thread.sleep(waitMs);
               } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  throw new SQLException("Interrupted while waiting for deadlock retry", ie);
               }
            } else {
               // Not a deadlock or no retries left - throw immediately
               throw ex;
            }
         }
      }
      
      // Should never reach here, but added for completeness
      throw new SQLException("Failed to insert data after " + MAX_RETRIES + 
              " retries due to SQL Server deadlocks (error 1205)", lastException);
   }

   /**
    * Internal method that performs the actual bulk copy operation.
    * Separated from insertDataToTable to enable retry logic for deadlock errors.
    * 
    * <p><strong>Deadlock Handling:</strong> SQL Server bulk inserts can deadlock under
    * high parallel load. The caller (insertDataToTable) implements retry logic with
    * exponential backoff and jitter to handle error 1205.</p>
    */
   private int insertDataToTableInternal(ResultSet resultSet, int taskId) throws SQLException {
      String tableName;

      // Get table name and columns
      if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
         tableName = getSinkTableName();
      } else {
         tableName = getQualifiedStagingTableName();
      }

      ResultSetMetaData rsmd = resultSet.getMetaData();
      int columnCount = rsmd.getColumnCount();

        String[] sinkColumnsArray = null;
        if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
          sinkColumnsArray = Arrays.stream(this.options.getSinkColumns().replace("\"", "").split(","))
             .map(String::trim)
             .toArray(String[]::new);
          if (sinkColumnsArray.length != columnCount) {
            throw new IllegalArgumentException(String.format(
               "Sink columns count (%d) does not match source column count (%d). Verify --sink-columns and --source-columns alignment.",
               sinkColumnsArray.length,
               columnCount));
          }
        }

      // Retrieve sink column types for type-aware mapping
      java.util.Map<Integer, Integer> sinkColumnTypes = getSinkColumnTypes(tableName, rsmd);

      java.util.Map<String, Integer> sinkColumnOrdinals = null;
      if (sinkColumnsArray != null) {
         sinkColumnOrdinals = getSinkColumnOrdinals(tableName);
      }

      SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(this.getConnection());
      // BulkCopy Options
      SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
      copyOptions.setBulkCopyTimeout(0);
      copyOptions.setBatchSize(options.getFetchSize());
      
      // Use TABLOCK to acquire table-level lock and prevent deadlocks during parallel operations
      // This is the recommended approach for parallel bulk inserts to avoid row-level lock contention
      copyOptions.setTableLock(true);
      
      bulkCopy.setBulkCopyOptions(copyOptions);

      bulkCopy.setDestinationTableName(tableName);

      // Columns Mapping
      if (sinkColumnsArray != null) {
         for (int i = 1; i <= sinkColumnsArray.length; i++) {
            String sinkCol = sinkColumnsArray[i - 1];
            Integer destOrdinal = sinkColumnOrdinals != null ? sinkColumnOrdinals.get(sinkCol.toLowerCase()) : null;
            if (destOrdinal == null) {
               throw new IllegalArgumentException(String.format(
                   "Sink column '%s' not found in destination table '%s'.", sinkCol, tableName));
            }
            // Map source ordinal to destination ordinal to skip extra sink columns
            bulkCopy.addColumnMapping(i, destOrdinal);
            LOG.debug("Column mapping: source index {} --> sink column {} (dest ordinal {})", i, sinkCol, destOrdinal);
         }
      } else {
         for (int i = 1; i <= columnCount; i++) {
            bulkCopy.addColumnMapping(rsmd.getColumnName(i), i);
         }
      }

      LOG.debug("Performing BulkCopy into {} ", tableName);
      try {
         // Write from the source to the destination.
         // If the source is a RowSet (e.g., MongoDB, CSV file), use the adapter
         // to handle type conversions (like Boolean to Integer for BIT columns)
         if (resultSet instanceof RowSet) {
            bulkCopy.writeToServer(new SQLServerBulkRecordAdapter((RowSet) resultSet));
         } else {
            // Pass sink column types to adapter for type-aware BulkCopy
            bulkCopy.writeToServer(new SQLServerResultSetBulkRecordAdapter(resultSet, sinkColumnTypes, null));
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
               try {
                  // get source column metadata - reflection into driver internals
                  Field fi = SQLServerBulkCopy.class.getDeclaredField("srcColumnMetadata");
                  fi.setAccessible(true);
                  @SuppressWarnings("unchecked") // Safe cast for MS SQL Server JDBC driver 12.x internal structure
                  HashMap<Integer, Object> srcColumnsMetadata = (HashMap<Integer, Object>) fi.get(bulkCopy);
                  
                  // get destination column metadata
                  fi = SQLServerBulkCopy.class.getDeclaredField("destColumnMetadata");
                  fi.setAccessible(true);
                  @SuppressWarnings("unchecked") // Safe cast for MS SQL Server JDBC driver 12.x internal structure  
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

                  // Log only mismatched columns at ERROR level for diagnostics
                  if (srcColumnPrecision != destPrecision || srcColumnType != destJdbcType) {
                     LOG.error("Column length mismatch: colid {} source {} {}({},{}) != sink {} {}({},{})", 
                        key, srcColumnName, srcType, srcColumnPrecision, srcColumnScale, 
                        destColumnName, destType, destPrecision, destScale);
                  }

               }
               } catch (NoSuchFieldException | IllegalAccessException e) {
                  LOG.warn("Failed to extract column metadata via reflection - SQL Server driver version may have changed: {}", e.getMessage());
               }
            }
         } catch (Exception e) {
            LOG.error("Error logging column length error: {}", e.getMessage());
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
   protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
      switch (jdbcType) {
         // Character types
         case java.sql.Types.CHAR:
            return precision > 0 && precision <= 8000 ? "CHAR(" + precision + ")" : "CHAR(255)";
         case java.sql.Types.VARCHAR:
         case java.sql.Types.LONGVARCHAR:
            if (precision <= 0 || precision > 8000) {
               return "VARCHAR(MAX)";
            }
            return "VARCHAR(" + precision + ")";
         case java.sql.Types.NCHAR:
            return precision > 0 && precision <= 4000 ? "NCHAR(" + precision + ")" : "NCHAR(255)";
         case java.sql.Types.NVARCHAR:
         case java.sql.Types.LONGNVARCHAR:
            if (precision <= 0 || precision > 4000) {
               return "NVARCHAR(MAX)";
            }
            return "NVARCHAR(" + precision + ")";
         
         // Numeric types
         case java.sql.Types.TINYINT:
            return "TINYINT";
         case java.sql.Types.SMALLINT:
            return "SMALLINT";
         case java.sql.Types.INTEGER:
            return "INT";
         case java.sql.Types.BIGINT:
            return "BIGINT";
         case java.sql.Types.DECIMAL:
         case java.sql.Types.NUMERIC:
            if (precision > 0 && scale >= 0) {
               return "DECIMAL(" + Math.min(precision, 38) + "," + Math.min(scale, 38) + ")";
            }
            return "DECIMAL(18,0)";
         case java.sql.Types.REAL:
            return "REAL";
         case java.sql.Types.FLOAT:
         case java.sql.Types.DOUBLE:
            return "FLOAT";
         
         // Date/Time types
         case java.sql.Types.DATE:
            return "DATE";
         case java.sql.Types.TIME:
         case java.sql.Types.TIME_WITH_TIMEZONE:
            return "TIME";
         case java.sql.Types.TIMESTAMP:
         case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
            return "DATETIME2";
         
         // Binary types
         case java.sql.Types.BINARY:
            return precision > 0 && precision <= 8000 ? "BINARY(" + precision + ")" : "BINARY(255)";
         case java.sql.Types.VARBINARY:
         case java.sql.Types.LONGVARBINARY:
            if (precision <= 0 || precision > 8000) {
               return "VARBINARY(MAX)";
            }
            return "VARBINARY(" + precision + ")";
         case java.sql.Types.BLOB:
            return "VARBINARY(MAX)";
         
         // Other types
         case java.sql.Types.BOOLEAN:
         case java.sql.Types.BIT:
            return "BIT";
         case java.sql.Types.CLOB:
            return "VARCHAR(MAX)";
         case java.sql.Types.NCLOB:
            return "NVARCHAR(MAX)";
         case java.sql.Types.ROWID:
            return "UNIQUEIDENTIFIER";
         case java.sql.Types.SQLXML:
            return "XML";
         
         default:
            LOG.warn("Unmapped JDBC type {} for column '{}', defaulting to NVARCHAR(MAX)", jdbcType, columnName);
            return "NVARCHAR(MAX)";
      }
   }

   @Override
   public void preSourceTasks () throws Exception {
      // Call parent to probe source metadata if auto-create is enabled
      super.preSourceTasks();
   }

   @Override
   public void postSourceTasks () {/*Not implemented*/}

   @Override
   public void postSinkTasks () throws Exception {
      setIdentityInsert(getSinkTableName(), true);
      super.postSinkTasks();
      setIdentityInsert(getSinkTableName(), false);
   }
}
