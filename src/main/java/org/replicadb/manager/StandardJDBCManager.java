package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;


public class StandardJDBCManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(StandardJDBCManager.class.getName());
   private static Long chunkSize = 0L;

   public StandardJDBCManager (ToolOptions opts, DataSourceType dsType) {
      super(opts);
      this.dsType = dsType;
   }

   @Override
   public String getDriverClass () {
      if (this.dsType.equals(DataSourceType.SOURCE)) {
         return this.options.getSourceConnectionParams().getProperty("driver");
      } else {
         return this.options.getSinkConnectionParams().getProperty("driver");
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
         sqlCmd = "SELECT  * FROM (" +
             options.getSourceQuery() + ") as T1 OFFSET ? ";
      } else {

         sqlCmd = "SELECT " +
             allColumns +
             " FROM " +
             escapeTableName(tableName);

         // Source Where
         if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
         }

         sqlCmd = sqlCmd + " OFFSET ? ";

      }

      String limit = " LIMIT ?";

      if (this.options.getJobs() == nThread + 1) {
         return super.execute(sqlCmd, offset);
      } else {
         sqlCmd = sqlCmd + limit;
         return super.execute(sqlCmd, offset, chunkSize);
      }

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

      Statement statement = this.getConnection().createStatement();
      String sinkStagingTable = getQualifiedStagingTableName();

      // Get sink columns.
      String allSinkColumns;
      if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
         allSinkColumns = this.options.getSinkColumns();
      } else if (this.options.getSourceColumns() != null && !this.options.getSourceColumns().isEmpty()) {
         allSinkColumns = this.options.getSourceColumns();
      } else {
         allSinkColumns = "*";
      }

      String sql = " CREATE TABLE " + sinkStagingTable + " AS SELECT " + allSinkColumns + " FROM " + this.getSinkTableName() + " WHERE 1 = 0 ";

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

      sql.append(" ) WHEN MATCHED THEN UPDATE SET ");

      // Set all columns for UPDATE SET statement
      for (String colName : allColls.split("\\s*,\\s*")) {

         boolean contains = Arrays.asList(pks).contains(colName);
         boolean containsUppercase = Arrays.asList(pks).contains(colName.toUpperCase());
         boolean containsQuoted = Arrays.asList(pks).contains("\"" + colName.toUpperCase() + "\"");
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
   public void preSourceTasks () throws SQLException {
      // Because chunkSize is static it's required to initialize it
      // when the unit tests are running
      chunkSize = 0L;

      // Only calculate the chunk size when parallel execution is active
      if (this.options.getJobs() != 1) {
          // Calculating the chunk size for parallel job processing
         Statement statement = this.getConnection().createStatement();
         String sql = "SELECT " + " CEIL(count(*) / " + options.getJobs() + ") chunk_size" + ", count(*) total_rows" + " FROM ";

         // Source Query
         if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            sql = sql + "( " + this.options.getSourceQuery() + " ) as REPLICADB_TABLE";

         } else {

            sql = sql + this.options.getSourceTable();
            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
               sql = sql + " WHERE " + options.getSourceWhere();
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
   public void postSourceTasks () throws Exception {
      // Not necessary
   }

}
