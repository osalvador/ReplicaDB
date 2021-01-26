package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import io.debezium.data.Envelope;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class SQLServerManagerCDC extends SQLServerManager implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

    private static final Logger LOG = LogManager.getLogger(SQLServerManagerCDC.class.getName());
    private static final String DEBEZIUM_CONNECTOR_CLASS="io.debezium.connector.sqlserver.SqlServerConnector";

    /**
     * Constructs the SqlManager.
     *
     * @param opts   the ReplicaDB ToolOptions describing the user's requested action.
     * @param dsType
     */
    public SQLServerManagerCDC(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
    }

    @Override
    public Properties getDebeziumProps() {
        // Define the configuration for the embedded and SQLServer connector ...
        Properties extraConnectionProps =this.options.getSourceConnectionParams();
        final Properties props = new Properties();

        /* begin engine properties */
        props.setProperty("name", "ReplicaDB-SQLServerCDC");
        // Default values
        props.setProperty("offset.flush.interval.ms", "5000");
        props.setProperty("offset.flush.timeout.ms", "15000");
        props.setProperty("max.batch.size", "1000");
        props.setProperty("max.queue.size", "2000");
        props.setProperty("query.fetch.size", "2000");
        props.setProperty("query.fetch.size", String.valueOf(this.options.getFetchSize()));
        props.setProperty("snapshot.fetch.size", "2000");

        props.setProperty("snapshot.isolation.mode", "read_committed");
        props.setProperty("snapshot.mode", "schema_only");
        //props.setProperty("provide.transaction.metadata", "true");
        props.setProperty("tombstones.on.delete", "false"); // only a delete event is sent.
        props.setProperty("converter.schemas.enable", "false"); // don't include schema in message
        /* connector properties */
        props.setProperty("connector.class", DEBEZIUM_CONNECTOR_CLASS);
        props.setProperty("database.server.name", "ReplicaDB_SQLServerCDC");
        //props.setProperty("database.hostname", "localhost");
        //props.setProperty("database.port", "1433");
        //props.setProperty("database.dbname", "BikeStores");
        props.setProperty("database.user", this.options.getSourceUser());
        props.setProperty("database.password", this.options.getSourcePassword());
        //props.setProperty("schema.include.list", "production");
        props.setProperty("table.include.list", this.options.getSourceTable() != null ? this.options.getSourceTable() : "");

        /* File names */
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");

        String fileNamePattern = "ReplicaDB_SQLServerCDC_%s_%s.dat";
        String dbHistoriFileName = String.format(fileNamePattern, extraConnectionProps.getProperty("database.dbname"),"dbhistory");
        props.setProperty("database.history.file.filename", "data/"+dbHistoriFileName);

        String offsetFileName = String.format(fileNamePattern, extraConnectionProps.getProperty("database.dbname"),"offset");
        props.setProperty("offset.storage.file.filename", "data/"+offsetFileName);

        props.putAll(this.options.getSourceConnectionParams());

        return props;
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
            System.out.println(record);
            if (record.value() != null) {
                Envelope.Operation operation = Envelope.operationFor(record);
                if (operation != null) {
                    switch (operation) {
                        case READ:
                            LOG.info("Read event. Snapshoting - Insert?");
                            break;
                        case CREATE:
                            LOG.info("Create event. Insert");
                            //doInsert(record);
                            break;
                        case DELETE:
                            LOG.info("Delete event. Delete");
                            break;
                        case UPDATE:
                            LOG.info("Update event. Update");
                            break;
                        default:
                            break;
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
            committer.markProcessed(r);
        }

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

    private void doInsert(SourceRecord record) throws SQLException {
        Connection conn = getConnection();
        Statement st = conn.createStatement();
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO " +
                getSourceTableName(record.value()) +
                "_copy VALUES (1, 'Oscar')");
        LOG.info("Inserting data with " + sql);
        st.executeUpdate(sql.toString());
        conn.commit();
        conn.close();
    }

     public Connection getConnection() throws SQLException {
        String url = "jdbc:sqlserver://localhost:1433;database=BikeStores";
        Connection conn = DriverManager.getConnection(url, "sa", "Eroski1234");
        return conn;
    }

    private String getSourceTableName(Object recordValue) {
        Struct struct = (Struct) recordValue;
        // get source
        String schemaName = ((Struct) struct.getStruct("source")).getString("schema");
        String tableName = ((Struct) struct.getStruct("source")).getString("table");
        return schemaName + "." + tableName;
    }
}
