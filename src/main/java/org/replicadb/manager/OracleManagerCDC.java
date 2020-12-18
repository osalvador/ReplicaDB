package org.replicadb.manager;

import io.debezium.data.Envelope;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OracleManagerCDC extends OracleManager implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

    private static final Logger LOG = LogManager.getLogger(OracleManagerCDC.class.getName());

    /**
     * Constructs the SqlManager.
     *
     * @param opts   the ReplicaDB ToolOptions describing the user's requested action.
     * @param dsType
     */
    public OracleManagerCDC(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
    }


    @Override
    public void handleBatch(List<RecordChangeEvent<SourceRecord>> records, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException {
        System.out.println("records.size:" + records.size());

        for (RecordChangeEvent<SourceRecord> r : records) {
            SourceRecord record = r.record();
            /*Schema valueSchema = record.valueSchema();

            for (Field field : valueSchema.field("after").schema().fields()) {
                System.out.println(field.toString());
                System.out.println(field.name());
                System.out.println(field.schema().type().getName());
            }
            System.out.println("....");

             */
            if (record.value() != null) {
                System.out.println(record);
                Envelope.Operation operation = Envelope.operationFor(record);
                if (operation != null) {
                    try {
                        switch (operation) {
                            case READ:
                                LOG.info("Read event. Snapshoting - Merge?");
                                //doInsert(record);
                                break;
                            case CREATE:
                                LOG.info("Create event. Insert");
                                doInsert(record);
                                break;
                            case DELETE:
                                LOG.info("Delete event. Delete");
                                doDelete(record);
                                break;
                            case UPDATE:
                                LOG.info("Update event. Update");
                                break;
                            default:
                                break;
                        }
                    } catch (Exception throwables) {
                        throwables.printStackTrace();
                    }
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


            //committer.markProcessed(r);
        }

        // Commit transactions
        LOG.debug("Commiting all records");
        try {
            this.getConnection().commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        committer.markBatchFinished();
        /*try {
            for (SourceRecord record : records) {
                try {
                    System.out.println(record);
                    recordCommitter.markProcessed(record);
                }
                catch (StopConnectorException ex) {
                    // ensure that we mark the record as finished
                    // in this case
                    recordCommitter.markProcessed(record);
                    throw ex;
                }
            }
        }
        finally {
            recordCommitter.markBatchFinished();
        }
*/
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

    private void doDelete(SourceRecord record) throws SQLException {
        String sinkTableName = options.getSinkStagingSchema() + "." + getSourceTableName(record);
        String[] pks = getSinkPrimaryKeys(sinkTableName);
        System.out.println(Arrays.toString(pks));


        StringBuilder sqlCmd = new StringBuilder();
        sqlCmd.append("DELETE " + sinkTableName + " WHERE ");
        for (int i = 0; i <= pks.length - 1; i++) {
            if (i > 0) sqlCmd.append(" AND ");
            sqlCmd.append(pks[i].toLowerCase()).append("=?");
        }

        LOG.info("Deleting record with " + sqlCmd);
        PreparedStatement ps = this.getConnection().prepareStatement(String.valueOf(sqlCmd));
        Struct value = ((Struct) record.value()).getStruct("before");
        for (int i = 0; i <= pks.length - 1; i++) {
            LOG.debug("{} - {}", i, value.get(pks[i].toLowerCase()));
            ps.setObject(i+1, value.get(pks[i].toLowerCase())); // TODO lowercase?
        }
        ps.execute();

    }

    private void doInsert(SourceRecord record) throws SQLException {

        List columns = getColumns(record);
        int columnsNumber = columns.size();
        String columnsNames = columns.stream().collect(Collectors.joining(",")).toString();

        String sinkTableName = options.getSinkStagingSchema() + "." + getSourceTableName(record);

        StringBuilder sqlCmd = new StringBuilder();
        sqlCmd.append("INSERT INTO ");
        sqlCmd.append(sinkTableName);
        sqlCmd.append(" (");
        sqlCmd.append(columnsNames);
        sqlCmd.append(")");
        sqlCmd.append(" VALUES ( ");
        for (int i = 0; i <= columnsNumber - 1; i++) {
            if (i > 0) sqlCmd.append(",");
            sqlCmd.append("?");
        }
        sqlCmd.append(" )");

        LOG.info("Inserting data with " + sqlCmd);

        PreparedStatement ps = this.getConnection().prepareStatement(String.valueOf(sqlCmd));

        Struct value = ((Struct) record.value()).getStruct("after");
        int i = 0;
        for (Field field : value.schema().fields()) {
            i++;
            ps.setObject(i, value.get(field));
            //System.out.println(field.name() + "=" + field.schema().type().getName());
            //System.out.println(field.name() + "=" + value.get(field));
        }

        ps.execute();

    }


    private List getColumns(SourceRecord record) {
        Struct value = ((Struct) record.value()).getStruct("after");

        List<String> columnNames = new ArrayList<>();
        int i = 0;
        for (Field field : value.schema().fields()) {
            i++;
            //if (i > 1) columnNames.append(",");
            columnNames.add(field.name());

            //System.out.println(field.name() + "=" + field.schema().type().getName());
            //System.out.println(field.name() + "=" + value.get(field));
        }
        return columnNames;

    }

    private String getSourceTableName(SourceRecord recordValue) {
        Struct struct = (Struct) recordValue.value();
        // get source
        //String schemaName = ((Struct) struct.getStruct("source")).getString("schema");
        String tableName = ((Struct) struct.getStruct("source")).getString("table");
        //return schemaName + "." + tableName;
        return tableName;
    }
}
