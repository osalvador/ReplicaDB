package org.replicadb.manager;

import com.google.common.io.CharSource;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.input.ReaderInputStream;
import org.mariadb.jdbc.MariaDbStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

public class MySQLManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(MySQLManager.class.getName());


   private static Long chunkSize = 0L;

   /**
    * Constructs the SqlManager.
    *
    * @param opts the ReplicaDB ToolOptions describing the user's requested action.
    */
   public MySQLManager (ToolOptions opts, DataSourceType dsType) {
      super(opts);
      this.dsType = dsType;
      // In MySQL and MariaDB this properties are required
      if (this.dsType.equals(DataSourceType.SINK)) {
         Properties mysqlProps = new Properties();
         mysqlProps.setProperty("characterEncoding", "UTF-8");
         mysqlProps.setProperty("allowLoadLocalInfile", "true");
         mysqlProps.setProperty("rewriteBatchedStatements", "true");
         options.setSinkConnectionParams(mysqlProps);
      }
   }

   @Override
   public String getDriverClass () {
      return JdbcDrivers.MYSQL.getDriverClass();
   }

   @Override
   public int insertDataToTable (ResultSet resultSet, int taskId) throws SQLException, IOException {
      int totalRows = 0;
      try {

         ResultSetMetaData rsmd = resultSet.getMetaData();
         String tableName;

         // Get table name and columns
         if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
            tableName = getSinkTableName();
         } else {
            tableName = getQualifiedStagingTableName();
         }

         String allColumns = getAllSinkColumns(rsmd);

         // Get MySQL LOAD DATA manager
         String loadDataSql = getLoadDataSql(tableName, allColumns, rsmd);
         PreparedStatement statement = this.connection.prepareStatement(loadDataSql);

         JdbcPreparedStatement mysqlStatement = null;
         MariaDbStatement mariadbStatement = null;
         if (statement.isWrapperFor(MariaDbStatement.class)) {
            mariadbStatement = statement.unwrap(MariaDbStatement.class);
         } else {
            mysqlStatement = statement.unwrap(JdbcPreparedStatement.class);
         }

         char unitSeparator = 0x1F;
         char nullAscii = 0x00;
         int columnsNumber = rsmd.getColumnCount();

         StringBuilder row = new StringBuilder();
         StringBuilder cols = new StringBuilder();

         String colValue;
         int rowCounts = 0;
         int batchSize = options.getFetchSize();

         if (resultSet.next()) {
            // Create Bandwidth Throttling
            BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);

            do {
               bt.acquiere();

               // Get Columns values
               for (int i = 1; i <= columnsNumber; i++) {
                  if (i > 1) cols.append(unitSeparator);

                  switch (rsmd.getColumnType(i)) {
                     case Types.CLOB:
                        colValue = clobToString(resultSet.getClob(i));
                        break;
                     case Types.BINARY:
                     case Types.VARBINARY:
                        colValue = byteToHex(resultSet.getBytes(i));
                        break;
                     case Types.LONGVARBINARY:
                     case Types.BLOB:
                        colValue = blobToHex(getBlob(resultSet,i));
                        break;
                     default:
                        colValue = resultSet.getString(i);
                        if (colValue == null) colValue = String.valueOf(nullAscii);
                        break;
                  }

                  if (!resultSet.wasNull() || colValue != null) cols.append(colValue);
               }

               // Escape special chars
               if (Boolean.TRUE.equals(this.options.isSinkDisableEscape())) {
                  row.append(cols.toString().replace("\u0000", "\\N") // MySQL localInfile Null value
                  );
               } else {
                  row.append(cols.toString().replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\u0000", "\\N") // MySQL localInfile Null value
                  );
               }

               // Row ends with \n
               row.append("\n");

               // Copy data to mysql
               if (++rowCounts % batchSize == 0) {
                  copyData(loadDataSql, row, mariadbStatement, mysqlStatement);

                  // Clear StringBuilders
                  row.setLength(0); // set length of buffer to 0
                  row.trimToSize();
                  rowCounts = 0;
               }

               // Clear StringBuilders
               cols.setLength(0); // set length of buffer to 0
               cols.trimToSize();
               totalRows++;
            } while (resultSet.next());
         }

         // insert remaining records
         if (rowCounts != 0) {
            copyData(loadDataSql, row, mariadbStatement, mysqlStatement);
         }

      } catch (Exception e) {
         this.connection.rollback();
         throw e;
      }

      this.getConnection().commit();
      return totalRows;
   }

   /**
    * Copy data to MySQL/MariaDB using LOAD DATA LOCAL INFILE
    * @param row StringBuilder guaranteed non-null by insertDataToTable contract
    */
   @SuppressWarnings("null") // row parameter is guaranteed non-null by calling contract in insertDataToTable
   private void copyData (String loadDataSql, StringBuilder row, MariaDbStatement mariadbStatement, JdbcPreparedStatement mysqlStatement) throws IOException, SQLException {
      if (mysqlStatement != null) {
         InputStream inputStream = ReaderInputStream.builder()
            .setReader(CharSource.wrap(row).openStream())
            .setCharset(StandardCharsets.UTF_8)
            .get();
         mysqlStatement.setLocalInfileInputStream(inputStream);
         mysqlStatement.executeUpdate(loadDataSql);
      } else {
         assert mariadbStatement != null;
         InputStream inputStream = ReaderInputStream.builder()
            .setReader(CharSource.wrap(row).openStream())
            .setCharset(StandardCharsets.UTF_8)
            .get();
         mariadbStatement.setLocalInfileInputStream(inputStream);
         mariadbStatement.executeUpdate(loadDataSql);
      }
   }

   private String getLoadDataSql (String tableName, String allColumns, ResultSetMetaData rsmd) throws SQLException {
      StringBuilder loadDataSql = new StringBuilder();
      loadDataSql.append("LOAD DATA LOCAL INFILE 'dummy' INTO TABLE ");
      loadDataSql.append(tableName);
      loadDataSql.append(" CHARACTER SET UTF8 FIELDS TERMINATED BY X'1F' ");

      // loop through the columns, if it is a BLOB type, convert it into a variable
      if (allColumns != null) {
         loadDataSql.append(" (");
         String[] columns = allColumns.split(",");
         for (int i = 0; i < columns.length; i++) {
            String col = columns[i].trim(); // Trim whitespace from column names
            switch (rsmd.getColumnType(i + 1)) {
               case Types.BIT:
               case Types.BINARY:
               case Types.BLOB:
               case Types.VARBINARY:
               case Types.LONGVARBINARY:
                  loadDataSql.append("@").append(col).append(", ");
                  break;
               default:
                  loadDataSql.append(col).append(", ");
                  break;
            }
         }
         // remove two last chars
         loadDataSql.setLength(loadDataSql.length() - 2);
         loadDataSql.append(")");

         // SET variables with UNHEX
         String setPrefix = " SET ";
         for (int i = 0; i < columns.length; i++) {
            String col = columns[i].trim(); // Trim whitespace from column names
            switch (rsmd.getColumnType(i + 1)) {
               case Types.BIT:
               case Types.BINARY:
               case Types.BLOB:
               case Types.VARBINARY:
               case Types.LONGVARBINARY:
                  loadDataSql.append(setPrefix).append(col).append("=UNHEX(@").append(col).append(") ");
                  loadDataSql.append(", ");
                  setPrefix = "";
                  break;
               default:
                  break;
            }
         }
         // remove two last chars
         if (setPrefix.equals("")) loadDataSql.setLength(loadDataSql.length() - 2);
      }

      LOG.info("Loading data with this command: {}", loadDataSql);
      return loadDataSql.toString();
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

      String sql = " CREATE TABLE " + sinkStagingTable + " AS (SELECT " + allSinkColumns + " FROM " + this.getSinkTableName() + " WHERE 1 = 0 ) ";

      LOG.info("Creating staging table with this command: {}", sql);
      statement.executeUpdate(sql);
      statement.close();
      this.getConnection().commit();
   }

   @Override
   protected void mergeStagingTable () throws SQLException {
      Statement statement = this.getConnection().createStatement();

      try {
         String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
         // Primary key is required
         if (pks == null || pks.length == 0) {
            throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
         }

         // options.sinkColumns was set during the insertDataToTable
         String allColls = getAllSinkColumns(null);

         StringBuilder sql = new StringBuilder();
         sql.append("INSERT INTO ").append(this.getSinkTableName()).append(" (").append(allColls).append(" ) ").append(" SELECT ").append(allColls).append(" FROM ").append(this.getSinkStagingTableName()).append(" as excluded ON DUPLICATE KEY UPDATE ");

         // Set all columns for DO UPDATE SET statement
         for (String colName : allColls.split(",")) {
            sql.append(" ").append(colName).append(" = excluded.").append(colName).append(" ,");
         }
         // Delete the last comma
         sql.setLength(sql.length() - 1);

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

   @Override
   public ResultSet readTable (String tableName, String[] columns, int nThread) throws SQLException {

      // If table name parameter is null get it from options
      tableName = tableName == null ? this.options.getSourceTable() : tableName;

      // If columns parameter is null, get it from options
      String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

      long offset = nThread * chunkSize;
      String sqlCmd;

      // Read table with source-query option specified
      if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
         sqlCmd = "SELECT  * FROM (" + options.getSourceQuery() + ") AS REPLICADB_TABLE ";
      } else {

         sqlCmd = "SELECT " + allColumns + " FROM " + escapeTableName(tableName);

         // Source Where
         if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
         }
      }

      if (chunkSize != 0L) {
         sqlCmd = sqlCmd + " LIMIT ? OFFSET ?";
         return super.execute(sqlCmd, chunkSize, offset);
      } else {
         return super.execute(sqlCmd);
      }
   }

   @Override
   public void preSourceTasks () throws SQLException {
      // Call parent to probe source metadata if auto-create is enabled
      try {
          super.preSourceTasks();
      } catch (Exception e) {
          throw new SQLException("Failed to probe source metadata", e);
      }
      
      // Because chunkSize is static it's required to initialize it
      // when the unit tests are running
      chunkSize = 0L;

      // Only calculate the chunk size when parallel execution is active
      if (this.options.getJobs() != 1) {
         /*
          * Calculating the chunk size for parallel job processing
          */
         Statement statement = this.getConnection().createStatement();
         String sql = "SELECT " + " CEIL(count(*) / " + options.getJobs() + ") chunk_size" + ", count(*) total_rows" + " FROM ";

         // Source Query
         if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            sql = sql + "( " + this.options.getSourceQuery() + " ) as REPLICADB_TABLE";

         } else {
            // Source table
            sql = sql + this.options.getSourceTable();
         }
         // Source Where
         if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
               // SourceWhere should not be used when query is set
               LOG.warn("source-where is ignored when source-query is specified.");
            } else {
               sql = sql + " WHERE " + this.options.getSourceWhere();
            }
         }

         LOG.debug("Calculating the chunks size with this sql: {}", sql);
         ResultSet rs = statement.executeQuery(sql);
         rs.next();
         chunkSize = rs.getLong(1);
         long totalNumberRows = rs.getLong(2);
         LOG.debug("chunkSize: {} totalNumberRows: {}", chunkSize, totalNumberRows);

         statement.close();
         this.getConnection().commit();
      }
   }

   @Override
   public void postSourceTasks () {
      /*Not implemented*/
   }

   /*********************************************************************************************
    * From BLOB to Hexadecimal String for Mysql Copy
    * @return string representation of blob
    *********************************************************************************************/
   private String blobToHex (Blob blobData) throws SQLException {

      String returnData = "";

      if (blobData != null) {
         try {
            byte[] bytes = blobData.getBytes(1, (int) blobData.length());
            returnData = Hex.encodeHexString(bytes).toUpperCase();
         } finally {
            // The most important thing here is free the BLOB to avoid memory Leaks
            blobData.free();
         }
      }
      return returnData;
   }

   private String byteToHex (byte[] bytes) {
      String returnData = "";
      if (bytes != null) {
         returnData = Hex.encodeHexString(bytes).toUpperCase();
      }
      return returnData;
   }

   @Override
   protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
       switch (jdbcType) {
           case Types.INTEGER:
               return "INT";
           case Types.TINYINT:
               return "TINYINT";
           case Types.SMALLINT:
               return "SMALLINT";
           case Types.BIGINT:
               return "BIGINT";
           case Types.FLOAT:
           case Types.REAL:
               return "FLOAT";
           case Types.DOUBLE:
               return "DOUBLE";
            case Types.NUMERIC:
            case Types.DECIMAL:
                // Special case: Oracle REAL/DOUBLE PRECISION/FLOAT come through as NUMERIC with scale=-127
                // Map based on precision: FLOAT (p=63), DOUBLE (p=126), or DOUBLE (other)
                if (scale == -127) {
                    if (precision == 63) {
                        return "FLOAT";
                    } else {
                        return "DOUBLE";  // For precision=126 or other Oracle FLOATs
                    }
                }
                if (precision > 0) {
                    return "DECIMAL(" + precision + ", " + scale + ")";
                } else {
                    return "DECIMAL";
                }
           case Types.VARCHAR:
           case Types.NVARCHAR:
           case Types.LONGVARCHAR:
               if (precision > 16383) {
                   return "LONGTEXT";
               } else if (precision > 0) {
                   return "VARCHAR(" + precision + ")";
               } else {
                   return "LONGTEXT";
               }
           case Types.CHAR:
           case Types.NCHAR:
               return "CHAR(" + precision + ")";
           case Types.BOOLEAN:
           case Types.BIT:
               return "BOOLEAN";
           case Types.DATE:
               return "DATE";
           case Types.TIME:
           case Types.TIME_WITH_TIMEZONE:
               return "TIME";
           case Types.TIMESTAMP:
           case Types.TIMESTAMP_WITH_TIMEZONE:
               return "DATETIME";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                // MariaDB/MySQL have row size limit of ~65KB for all columns combined
                // Use BLOB types for large binary data to avoid "Column length too big" errors
                // Conservative threshold: use BLOB for columns > 16KB to leave room for other columns
                if (precision > 16384) {
                    return "LONGBLOB";
                } else if (precision > 0) {
                    return "VARBINARY(" + precision + ")";
                } else {
                    return "LONGBLOB";
                }
           case Types.BLOB:
               return "LONGBLOB";
           case Types.CLOB:
               return "LONGTEXT";
           default:
               LOG.warn("Unmapped JDBC type {} for column {}, using LONGTEXT as fallback", jdbcType, columnName);
               return "LONGTEXT";
       }
   }

}
