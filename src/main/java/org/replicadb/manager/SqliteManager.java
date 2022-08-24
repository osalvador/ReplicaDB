package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;


public class SqliteManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(SqliteManager.class.getName());
   private static Long chunkSize = 0L;

   public SqliteManager (ToolOptions opts, DataSourceType dsType) {
      super(opts);
      this.dsType = dsType;
      if (dsType.equals(DataSourceType.SINK) && options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
         throw new NotSupportedException("The complete-atomic mode is not supported in SQLite database as sink.");
      }
   }

   @Override
   public String getDriverClass () {
      return JdbcDrivers.SQLITE.getDriverClass();
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

      sqlCmd = sqlCmd + " LIMIT ? OFFSET ? ";

      if (this.options.getJobs() == nThread + 1) {
         return super.execute(sqlCmd, "-1", offset);
      } else {
         return super.execute(sqlCmd, chunkSize, offset );
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
                     if (blobData != null){
                        ps.setBytes(i, blobData.getBytes(1, (int) blobData.length()));
                        blobData.free();
                     } else ps.setNull(i,Types.BLOB);
                     break;
                  case Types.CLOB:
                     Clob clobData = resultSet.getClob(i);
                     ps.setString(i, clobToString(clobData));
                     if (clobData != null) clobData.free();
                     break;
                  case Types.BOOLEAN:
                     ps.setBoolean(i, resultSet.getBoolean(i));
                     break;
                  case Types.NVARCHAR:
                     ps.setNString(i, resultSet.getNString(i));
                     break;
                  case Types.ROWID:
                     ps.setRowId(i, resultSet.getRowId(i));
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
         sql.append("INSERT INTO ")
             .append(this.getSinkTableName())
             .append(" (")
             .append(allColls)
             .append(" ) ")
             .append(" SELECT ")
             .append(allColls)
             .append(" FROM ")
             .append(this.getSinkStagingTableName())
             .append(" WHERE true ON CONFLICT ")
             .append(" (").append(String.join(",", pks)).append(" )")
             .append(" DO UPDATE SET ");

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

   @Override
   protected void truncateTable () throws SQLException {
      String tableName;
      // Get table name
      if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())
          || options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
         tableName = getQualifiedStagingTableName();
      } else {
         tableName = getSinkTableName();
      }
      String sql = "DELETE FROM " + tableName;
      LOG.info("Truncating sink table with this command: {}", sql);
      Statement statement = this.getConnection().createStatement();
      statement.executeUpdate(sql);
      statement.close();
      this.getConnection().commit();
   }
}
