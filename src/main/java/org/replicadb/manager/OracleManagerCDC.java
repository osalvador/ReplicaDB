package org.replicadb.manager;

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
import org.replicadb.time.Conversions;

import javax.xml.transform.Source;
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
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        mapTables();
    }

    private void mapTables() {
        String[] sourceTables = options.getSourceTable().split(",");
        String[] sinkTables = options.getSinkTable().split(",");
        for (int i = 0; i < sourceTables.length; i++) {
            mappingSourceSinkTables.put(sourceTables[i].trim(), sinkTables[i].trim());
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

                                int[] rows = batchPS.executeBatch();
                                this.getConnection().commit();
                                LOG.info("Commited batch records. Rows affected: {}", rows.length);
                                batchPS.close();
                                batchPS = null;
                            }
                        }

                        switch (operation) {
                            case READ:
                                LOG.trace("Read event. Snapshoting - Insert?? Merge??");
                                doInsert(record);
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
                        throwables.printStackTrace();
                    }
                    oldOperation = operation;
                    oldSinkTableName = getSourceTableName(record);
                }
            }
            /*
            // table settings
            String table = getSourceTableName(record.value());
            System.out.println(table);

            Struct value = ((Struct) record.value()).getStruct("after");

            for (Field field : value.schema().fields()) {
                System.out.println(field.name() + "=" + field.schema().type().getName());
                System.out.println(field.name() + "=" + value.get(field));
            }
            System.out.println("----");


            //System.out.println("Key = '" + record.key() + "' value = '" + record.value() + "'");

*/


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
            throwables.printStackTrace();
        } finally {
            try {
                if (batchPS != null)
                    batchPS.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            batchPS = null;
        }
        committer.markBatchFinished();

        // for (SourceRecord record : list) {
        //System.out.println(record.value());

//            for (Field campo : record.valueSchema().fields()){
//                System.out.println(campo.name());
//               /* before
//                 after
//                 source
//                 op
//                ts_ms */
//            }

        //       Struct struct = (Struct)record.value();

        //     Struct after = (Struct)struct.get("after");

        //   System.out.println(after.schema().fields());


        // recordCommitter.markProcessed(record);
        //  }

//        recordCommitter.markBatchFinished();

    }

    private void doUpdate(SourceRecord record) throws SQLException {

        String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record));
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
        String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record));
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
            String sinkTableName = mappingSourceSinkTables.get(getSourceTableName(record));
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

    private void bindValuesToBatchPS(Struct value) throws SQLException {
        String fieldName = null;
        String filedSchemaName = null;
        int i = 0;
        for (Field field : value.schema().fields()) {
            i++;
            fieldName = field.name();
            filedSchemaName = field.schema().name();

            //LOG.debug("Field name: {}, Schema type:{}, Schema Name:{}", fieldName, field.schema().type(), field.schema().name());

            switch (field.schema().type()) {
                case STRING:
                    if (filedSchemaName != null && filedSchemaName.equals("io.debezium.data.Xml")) {
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
                    } else if (filedSchemaName != null) {
                        switch (filedSchemaName) {
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
                    if (filedSchemaName != null) {
                        if (int64Value == null) {
                            batchPS.setNull(i, Types.TIMESTAMP);
                        } else {
                            switch (filedSchemaName) {
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
