package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.sql.*;

/**
 * Generic JDBC manager for databases without specialized implementations.
 * Used as fallback when no database-specific manager is available.
 * 
 * <p><b>NULL Handling:</b> Correctly preserves NULL values for all data types by checking
 * {@code ResultSet.wasNull()} after primitive getters and calling {@code PreparedStatement.setNull()}
 * when NULL is detected. Prevents silent NULL-to-default-value conversions.</p>
 * 
 * <p>Includes explicit handling for BOOLEAN and NVARCHAR types in addition to standard
 * primitive types (INTEGER, BIGINT, DOUBLE, FLOAT, DATE, TIMESTAMP, VARCHAR, BINARY).</p>
 * 
 * @see OracleManager For detailed NULL handling pattern documentation
 * @see <a href="https://github.com/osalvador/ReplicaDB/issues/51">Issue #51</a>
 */
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
                     String strVal = resultSet.getString(i);
                     if (resultSet.wasNull() || strVal == null) {
                        ps.setNull(i, Types.VARCHAR);
                     } else {
                        ps.setString(i, strVal);
                     }
                     break;
                  case Types.INTEGER:
                  case Types.TINYINT:
                  case Types.SMALLINT:
                     int intVal = resultSet.getInt(i);
                     if (resultSet.wasNull()) {
                        ps.setNull(i, Types.INTEGER);
                     } else {
                        ps.setInt(i, intVal);
                     }
                     break;
                  case Types.BIGINT:
                  case Types.NUMERIC:
                  case Types.DECIMAL:
                     java.math.BigDecimal bdVal = resultSet.getBigDecimal(i);
                     if (resultSet.wasNull() || bdVal == null) {
                        ps.setNull(i, Types.NUMERIC);
                     } else {
                        ps.setBigDecimal(i, bdVal);
                     }
                     break;
                  case Types.DOUBLE:
                     double doubleVal = resultSet.getDouble(i);
                     if (resultSet.wasNull()) {
                        ps.setNull(i, Types.DOUBLE);
                     } else {
                        ps.setDouble(i, doubleVal);
                     }
                     break;
                  case Types.FLOAT:
                     float floatVal = resultSet.getFloat(i);
                     if (resultSet.wasNull()) {
                        ps.setNull(i, Types.FLOAT);
                     } else {
                        ps.setFloat(i, floatVal);
                     }
                     break;
                  case Types.DATE:
                     java.sql.Date dateVal = resultSet.getDate(i);
                     if (resultSet.wasNull() || dateVal == null) {
                        ps.setNull(i, Types.DATE);
                     } else {
                        ps.setDate(i, dateVal);
                     }
                     break;
                  case Types.TIMESTAMP:
                  case Types.TIMESTAMP_WITH_TIMEZONE:
                  case -101:
                  case -102:
                     java.sql.Timestamp tsVal = resultSet.getTimestamp(i);
                     if (resultSet.wasNull() || tsVal == null) {
                        ps.setNull(i, Types.TIMESTAMP);
                     } else {
                        ps.setTimestamp(i, tsVal);
                     }
                     break;
                  case Types.BINARY:
                  case Types.VARBINARY:
                  case Types.LONGVARBINARY:
                     byte[] bytesVal = resultSet.getBytes(i);
                     if (resultSet.wasNull() || bytesVal == null) {
                        ps.setNull(i, rsmd.getColumnType(i));
                     } else {
                        ps.setBytes(i, bytesVal);
                     }
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
                     boolean boolVal = resultSet.getBoolean(i);
                     if (resultSet.wasNull()) {
                        ps.setNull(i, Types.BOOLEAN);
                     } else {
                        ps.setBoolean(i, boolVal);
                     }
                     break;
                  case Types.NVARCHAR:
                     String nStrVal = resultSet.getNString(i);
                     if (resultSet.wasNull() || nStrVal == null) {
                        ps.setNull(i, Types.NVARCHAR);
                     } else {
                        ps.setNString(i, nStrVal);
                     }
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
   protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
      // Generic ANSI SQL types - use portable, widely-supported syntax
      switch (jdbcType) {
         // Character types
         case Types.CHAR:
            return precision > 0 && precision <= 2000 ? "CHAR(" + precision + ")" : "CHAR(255)";
         case Types.VARCHAR:
         case Types.LONGVARCHAR:
            if (precision <= 0 || precision > 4000) {
               return "VARCHAR(4000)";
            }
            return "VARCHAR(" + precision + ")";
         case Types.NCHAR:
            return precision > 0 && precision <= 2000 ? "NCHAR(" + precision + ")" : "NCHAR(255)";
         case Types.NVARCHAR:
         case Types.LONGNVARCHAR:
            if (precision <= 0 || precision > 4000) {
               return "NVARCHAR(4000)";
            }
            return "NVARCHAR(" + precision + ")";
         
         // Numeric types
         case Types.TINYINT:
            return "SMALLINT";
         case Types.SMALLINT:
            return "SMALLINT";
         case Types.INTEGER:
            return "INTEGER";
         case Types.BIGINT:
            return "BIGINT";
         case Types.DECIMAL:
         case Types.NUMERIC:
            if (precision > 0 && scale >= 0) {
               return "DECIMAL(" + Math.min(precision, 38) + "," + Math.min(scale, 38) + ")";
            }
            return "DECIMAL(18,0)";
         case Types.REAL:
            return "REAL";
         case Types.FLOAT:
            return "FLOAT";
         case Types.DOUBLE:
            return "DOUBLE PRECISION";
         
         // Date/Time types
         case Types.DATE:
            return "DATE";
         case Types.TIME:
         case Types.TIME_WITH_TIMEZONE:
            return "TIME";
         case Types.TIMESTAMP:
         case Types.TIMESTAMP_WITH_TIMEZONE:
            return "TIMESTAMP";
         
         // Binary types
         case Types.BINARY:
            return precision > 0 && precision <= 2000 ? "BINARY(" + precision + ")" : "BINARY(255)";
         case Types.VARBINARY:
         case Types.LONGVARBINARY:
            if (precision <= 0 || precision > 4000) {
               return "VARBINARY(4000)";
            }
            return "VARBINARY(" + precision + ")";
         case Types.BLOB:
            return "BLOB";
         
         // Other types
         case Types.BOOLEAN:
         case Types.BIT:
            return "BOOLEAN";
         case Types.CLOB:
            return "CLOB";
         case Types.NCLOB:
            return "NCLOB";
         case Types.ROWID:
            return "VARCHAR(40)";
         case Types.SQLXML:
            return "CLOB";
         
         default:
            LOG.warn("Unmapped JDBC type {} for column '{}', defaulting to VARCHAR(4000)", jdbcType, columnName);
            return "VARCHAR(4000)";
      }
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
      // Call parent to probe source metadata if auto-create is enabled
      super.preSourceTasks();
      // No other StandardJDBC-specific pre-source tasks needed.
   }
   @Override
   protected void mergeStagingTable () throws Exception {
      // Not necessary
   }
   @Override
   protected void truncateTable () throws SQLException {
      super.truncateTable("TRUNCATE TABLE ");
   }
}
