package org.replicadb.manager.cdc;

import io.debezium.data.Envelope;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.OracleManager;
import org.replicadb.time.Conversions;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class OracleManagerCDC extends OracleManager implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

    private static final Logger LOG = LogManager.getLogger(OracleManagerCDC.class.getName());

    private static PreparedStatement batchPS = null;
    private static HashMap<String, String> mappingSourceSinkTables = new HashMap<>();

    /**
     * Constructs the SqlManager.
     *
     * @param opts   the ReplicaDB ToolOptions describing the user's requested action.
     * @param dsType
     */
    public OracleManagerCDC(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
        //
        try {
            super.oracleAlterSession(false);

            // Disable PARALLEL DML
            Statement stmt = this.getConnection().createStatement();
            stmt.executeUpdate("ALTER SESSION DISABLE PARALLEL DML");
            stmt.close();
        } catch (SQLException throwables) {
            LOG.error(throwables);
        }

        mapTables();
    }

    private void mapTables() {
        String[] sourceTables = options.getSourceTable().split(",");
        String[] sinkTables = options.getSinkTable().split(",");
        for (int i = 0; i < sourceTables.length; i++) {
            mappingSourceSinkTables.put(sourceTables[i].trim().toLowerCase(), sinkTables[i].trim().toLowerCase());
            LOG.debug("Source Table -> Sink Table: {} -> {}", sourceTables[i].trim(),sinkTables[i].trim() );
        }
    }


    @Override
    public void handleBatch(List<RecordChangeEvent<SourceRecord>> records, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException {
        LOG.info("New records received: {}", records.size());

        // batch operations
        Envelope.Operation oldOperation = null;
        String oldSinkTableName = null;

        for (RecordChangeEvent<SourceRecord> r : records) {
            SourceRecord record = r.record();
            if (record.value() != null) {
                LOG.debug(record);
                Envelope.Operation operation = Envelope.operationFor(record);

                if (operation != null) {
                    try {
                        // if the operation OR de sink table changes execute bath operations
                        if (batchPS != null) {
                            LOG.trace("batchPS es nulo");
                            LOG.trace("oldOperation: {}, newOperation: {}", oldOperation, operation);
                            LOG.trace("oldSinkTableName: {}, newSinkTableName: {}", oldSinkTableName, getSourceTableName(record));
                            if (
                                    (oldOperation != null && !oldOperation.equals(operation))
                                            ||
                                            (oldSinkTableName != null && !oldSinkTableName.equals(getSourceTableName(record)))
                            ) {

                              try {
                                  int[] rows = batchPS.executeBatch();
                                  this.getConnection().commit();
                                  LOG.info("Commited batch records. Rows affected: {}", rows.length);
                                  batchPS.close();
                                  batchPS = null;
                              }catch (Exception e){
                                  LOG.error("Error ejecutando el batchPS.executeBatch: {}",e.getMessage());
                                  LOG.error(e);
                                  // TODO Si hay un error en un batchPS se hace rollback en todo el batch y perdemos datos.
                                  // hay que ver como gestionar este error

                              }
                            }
                        }

                        switch (operation) {
                            case READ:
                                LOG.trace("Read event. Snapshoting - Merge");
                                doMerge(record);
                                break;
                            case CREATE:
                                LOG.trace("Create event. Insert");
                                doInsert(record);
                                break;
                            case DELETE:
                                LOG.trace("Delete event. Delete");
                                doDelete(record);
                                break;
                            case UPDATE:
                                LOG.trace("Update event. Update");
                                doUpdate(record);
                                break;
                            default:
                                break;
                        }
                    } catch (Exception throwables) {
                        LOG.error("Error preparando la operacion: {}",throwables.getMessage());
                        LOG.error(throwables);
                    }
                    oldOperation = operation;
                    oldSinkTableName = getSourceTableName(record);
                }
            }

            committer.markProcessed(r);
        }

        // Commit transactions
        try {
            if (batchPS != null) {
                int[] rows = batchPS.executeBatch();
                this.getConnection().commit();
                LOG.info("Commited all records. Rows affected: {}", rows.length);
            }
        } catch (SQLException throwables) {
            LOG.error("Un SQLException");
            LOG.error(throwables);
        }catch (Exception e){
            LOG.error("Salgo por aqui");
            LOG.error("Error no controlado: {}", e.getMessage());
            LOG.error(e);
        } finally {
            try {
                if (batchPS != null)
                    batchPS.close();
            } catch (SQLException throwables) {
                LOG.error(throwables);
            }
            batchPS = null;
        }
        committer.markBatchFinished();

    }

    private void doUpdate(SourceRecord record) throws SQLException {

        String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record).toLowerCase());
        String[] pks = getSourcePrimaryKeys(record);
        List<String> columns = getColumns(record);

        if (batchPS == null) {
            StringBuilder sqlCmd = new StringBuilder();
            sqlCmd.append("UPDATE " + sinkTableName + " SET ");

            for (int i = 0; i <= columns.size() - 1; i++) {
                if (i > 0) sqlCmd.append(",");
                sqlCmd.append(columns.get(i)).append("=?");
            }
            sqlCmd.append(" WHERE ");
            for (int i = 0; i <= pks.length - 1; i++) {
                if (i > 0) sqlCmd.append(" AND ");
                sqlCmd.append(pks[i].toLowerCase()).append("=?");
            }

            LOG.info("Updating record with " + sqlCmd);
            batchPS = this.getConnection().prepareStatement(String.valueOf(sqlCmd));
        }

        // Binding values to the statement
        Struct value = ((Struct) record.value()).getStruct("after");
        // SET values
        bindValuesToBatchPS(value);
        int psIndex = columns.size() + 1;
        // WHERE values
        for (int i = 0; i <= pks.length - 1; i++) {
            try {
                batchPS.setObject(psIndex, value.get(pks[i]));
            } catch (DataException e) {
                try {
                    batchPS.setObject(psIndex, value.get(pks[i].toLowerCase()));
                } catch (DataException ex) {
                    batchPS.setObject(psIndex, value.get(pks[i].toUpperCase()));
                }
            }
            psIndex++;
        }
        batchPS.addBatch();
    }

    private void doDelete(SourceRecord record) throws SQLException {
        String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record).toLowerCase());
        String[] pks = getSourcePrimaryKeys(record);

        if (batchPS == null) {

            StringBuilder sqlCmd = new StringBuilder();
            sqlCmd.append("DELETE FROM " + sinkTableName + " WHERE ");
            for (int i = 0; i <= pks.length - 1; i++) {
                if (i > 0) sqlCmd.append(" AND ");
                sqlCmd.append(pks[i].toLowerCase()).append("=?");
            }

            LOG.info("Deleting record with " + sqlCmd);
            batchPS = this.getConnection().prepareStatement(String.valueOf(sqlCmd));
        }

        Struct value = ((Struct) record.value()).getStruct("before");
        for (int i = 0; i <= pks.length - 1; i++) {
            // TODO: If the PK is a date we may have an error?
            try {
                batchPS.setObject(i + 1, value.get(pks[i]));
            } catch (DataException e) {
                try {
                    batchPS.setObject(i + 1, value.get(pks[i].toLowerCase()));
                } catch (DataException ex) {
                    batchPS.setObject(i + 1, value.get(pks[i].toUpperCase()));
                }
            }
        }
        batchPS.addBatch();

    }

    private void doInsert(SourceRecord record) throws SQLException {

        if (batchPS == null) {

            List columns = getColumns(record);
            int columnsNumber = columns.size();
            String columnsNames = columns.stream().collect(Collectors.joining(",")).toString();
            String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record).toLowerCase());
            StringBuilder sqlCmd = new StringBuilder();
            sqlCmd.append(String.format("INSERT INTO %s ( %s ) VALUES ( ", sinkTableName, columnsNames));
            for (int i = 0; i <= columnsNumber - 1; i++) {
                if (i > 0) sqlCmd.append(",");
                sqlCmd.append("?");
            }
            sqlCmd.append(" )");

            LOG.info("Inserting data with " + sqlCmd);

            batchPS = this.getConnection().prepareStatement(String.valueOf(sqlCmd));
        }

        // Bind values to the sqlStatement
        Struct value = ((Struct) record.value()).getStruct("after");
        bindValuesToBatchPS(value);
        batchPS.addBatch();

    }

    private void doMerge(SourceRecord record) throws SQLException{
        String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record).toLowerCase());
        String[] pks = getSourcePrimaryKeys(record);
        List<String> columns = getColumns(record);

        if (batchPS == null) {
            StringBuilder sqlCmd = new StringBuilder();
            sqlCmd.append("MERGE INTO " + sinkTableName + " trg USING (SELECT ");

            for (int i = 0; i <= columns.size() - 1; i++) {
                if (i > 0) sqlCmd.append(",");
                sqlCmd.append("? as ").append(columns.get(i));
            }
            sqlCmd.append(" FROM DUAL) src ON (");

            for (int i = 0; i <= pks.length - 1; i++) {
                if (i > 0) sqlCmd.append(" AND ");
                sqlCmd.append("src.").append(pks[i]).append(" = trg.").append(pks[i]);
            }

            sqlCmd.append(" ) WHEN MATCHED THEN UPDATE SET ");
            // Set all columns for UPDATE SET statement
            for (int i = 0; i <= columns.size() - 1; i++) {
                boolean contains = Arrays.asList(pks).contains(columns.get(i));
                boolean containsUppercase = Arrays.asList(pks).contains(columns.get(i).toUpperCase());
                boolean containsQuoted = Arrays.asList(pks).contains("\""+columns.get(i).toUpperCase()+"\"");
                if (!contains && !containsUppercase && !containsQuoted)
                    sqlCmd.append(" trg.").append(columns.get(i)).append(" = src.").append(columns.get(i)).append(" ,");
            }
            // Delete the last comma
            sqlCmd.setLength(sqlCmd.length() - 1);

            sqlCmd.append(" WHEN NOT MATCHED THEN INSERT ( ");
            sqlCmd.append(String.join(",",columns));
            // Todas las columnas
            sqlCmd.append(" ) VALUES ( ");
            for (int i = 0; i <= columns.size() - 1; i++) {
                if (i > 0) sqlCmd.append(",");
                sqlCmd.append(" src.").append(columns.get(i));
            }
            sqlCmd.append(" ) ");

            LOG.info("Merging record with " + sqlCmd);
            batchPS = this.getConnection().prepareStatement(String.valueOf(sqlCmd));
        }

        // Binding values to the statement
        Struct value = ((Struct) record.value()).getStruct("after");
        // SET values
        bindValuesToBatchPS(value);
        batchPS.addBatch();
    }

    private void bindValuesToBatchPS(Struct value) throws SQLException {
        String fieldName = null;
        String fieldSchemaName = null;
        int i = 0;
        for (Field field : value.schema().fields()) {
            i++;
            fieldName = field.name();
            fieldSchemaName = field.schema().name();

            //LOG.debug("Field name: {}, Schema type:{}, Schema Name:{}", fieldName, field.schema().type(), field.schema().name());

            switch (field.schema().type()) {
                case STRING:
                    if (fieldSchemaName != null && fieldSchemaName.equals("io.debezium.data.Xml")) {
                        SQLXML xml = this.getConnection().createSQLXML();
                        xml.setString(value.getString(fieldName));
                        batchPS.setSQLXML(i, xml);
                        xml.free();
                    } else {
                        batchPS.setString(i, value.getString(fieldName));
                    }
                    break;
                case INT8:
                    batchPS.setObject(i, value.getInt8(fieldName), Types.INTEGER);
                    break;
                case INT16:
                    batchPS.setObject(i, value.getInt16(fieldName), Types.INTEGER);
                    break;
                case INT32:
                    Integer int32Value = value.getInt32(fieldName);
                    if (int32Value == null) {
                        batchPS.setNull(i, Types.INTEGER);
                    } else if (fieldSchemaName != null) {
                        switch (fieldSchemaName) {
                            case "io.debezium.time.Date":
                                batchPS.setDate(i, Conversions.sqlDateOfEpochDay(int32Value));
                                break;
                            case "io.debezium.time.Time":
                                batchPS.setObject(i, Conversions.timeOfMilliOfDay(int32Value));
                                break;
                            default:
                                batchPS.setInt(i, int32Value);
                                break;
                        }
                    } else {
                        batchPS.setInt(i, value.getInt32(fieldName));
                    }
                    break;
                case INT64:
                    Long int64Value = value.getInt64(fieldName);
                    if (fieldSchemaName != null) {
                        if (int64Value == null) {
                            batchPS.setNull(i, Types.TIMESTAMP);
                        } else {
                            switch (fieldSchemaName) {
                                case "io.debezium.time.NanoTime":
                                    batchPS.setObject(i, Conversions.timeOfNanoOfDay(int64Value), Types.TIMESTAMP);
                                    break;
                                case "io.debezium.time.MicroTime":
                                    batchPS.setObject(i, Conversions.timeOfMicroOfDay(int64Value), Types.TIMESTAMP);
                                    break;
                                case "io.debezium.time.Timestamp":
                                    batchPS.setObject(i, Conversions.timestampOfEpochMilli(int64Value), Types.TIMESTAMP);
                                    break;
                                case "io.debezium.time.MicroTimestamp":
                                    batchPS.setObject(i, Conversions.timestampOfEpochMicro(int64Value), Types.TIMESTAMP);
                                    break;
                                case "io.debezium.time.NanoTimestamp":
                                    batchPS.setObject(i, Conversions.timestampOfEpochNano(int64Value), Types.TIMESTAMP);
                                    break;
                                default:
                                    batchPS.setObject(i, BigDecimal.valueOf(int64Value), Types.NUMERIC);
                            }
                        }
                    } else {
                        if (int64Value == null) {
                            batchPS.setNull(i, Types.NUMERIC);
                        } else {
                            batchPS.setObject(i, BigDecimal.valueOf(int64Value), Types.NUMERIC);
                        }
                    }
                    break;
                case FLOAT32:
                    batchPS.setObject(i, value.getFloat32(fieldName), Types.REAL);
                    break;
                case FLOAT64:
                    batchPS.setObject(i, value.getFloat64(fieldName), Types.DOUBLE);
                    break;
                case BOOLEAN:
                    batchPS.setObject(i, value.getBoolean(fieldName), Types.BIT);
                    break;
                case BYTES:
                    if (value.get(fieldName) instanceof BigDecimal)
                        batchPS.setBigDecimal(i, (BigDecimal) value.get(fieldName));
                    else {
                        //Blob blob = this.getConnection().createBlob();
                        //blob.setBytes(1,value.getBytes(fieldName));
                        //batchPS.setBlob(i,blob);
                        //blob.free();
                        batchPS.setBytes(i, value.getBytes(fieldName));
                    }
                    break;
                case ARRAY:
                    // Debe ser probado
                    /*final List<String> arrayList = value.getArray(fieldName);
                    final String[] data = arrayList.toArray(new String[arrayList.size()]);
                    batchPS.setArray(i, this.getConnection().createArrayOf("VARCHAR",data));*/
                    batchPS.setObject(i, value.getArray(fieldName), Types.ARRAY);
                    break;
                case MAP:
                    batchPS.setObject(i, value.getMap(fieldName));
                    break;
                case STRUCT:
                    batchPS.setObject(i, value.getStruct(fieldName), Types.STRUCT);
                    break;
                default:
                    batchPS.setObject(i, value.getString(fieldName), Types.VARCHAR);
                    break;
            }


        }
    }

    private List<String> getColumns(SourceRecord record) {
        Struct value = ((Struct) record.value()).getStruct("after");

        List<String> columnNames = new ArrayList<>();
        int i = 0;
        for (Field field : value.schema().fields()) {
            i++;
            columnNames.add(field.name());
        }
        return columnNames;

    }

    private String getSourceTableName(SourceRecord recordValue) {
        Struct struct = (Struct) recordValue.value();
        String table =struct.getStruct("source").getString("table");
        String schema = struct.getStruct("source").getString("schema");
        // get source
        return schema + "." + table;
    }

    public String[] getSourcePrimaryKeys(SourceRecord record) {
        Struct key = (Struct) record.key();
        ArrayList<String> pks = new ArrayList<>();
        for (Field field : key.schema().fields()) {
            pks.add(field.name());
        }
        return pks.toArray(new String[0]);
    }
}
