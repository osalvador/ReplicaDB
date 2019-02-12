package org.replicadb.manager;

import org.apache.kafka.clients.producer.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.replicadb.cli.ToolOptions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Properties;

public class KafkaManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(KafkaManager.class.getName());

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public KafkaManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    protected Connection makeSinkConnection() {
        /*Not necessary for kafka*/
        return null;
    }

    @Override
    protected void truncateTable() {
        /*Not necessary for kafka*/
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.KAFKA.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws Exception {

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        String topic = options.getSinkTable();

        final Properties props = options.getSinkConnectionParams();
        // Add additional properties.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getSinkConnect().substring("kafka://".length()));
        props.put("topic", topic);

        LOG.debug("kafka properties: " + props);

        // Create kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create array form sink column names
        String[] sinkColumns = getAllSinkColumns(rsmd).split(",");

        while (resultSet.next()) {
            JSONObject obj;

            // Just one sink column and is a JSON.
            // Create a JSON object form content.
            if (sinkColumns[0].equals("json")) {
                obj = new JSONObject(resultSet.getString(1));
            } else {

                obj = new JSONObject();

                for (int i = 1; i < columnsNumber + 1; i++) {

                    String columnName = sinkColumns[i - 1];

                    switch (rsmd.getColumnType(i)) {
                        case Types.ARRAY:
                            obj.put(columnName, resultSet.getArray(i));
                            break;
                        case Types.BIGINT:
                            obj.put(columnName, resultSet.getInt(i));
                            break;
                        case Types.BOOLEAN:
                            obj.put(columnName, resultSet.getBoolean(i));
                            break;
                        case Types.BLOB:
                            obj.put(columnName, resultSet.getBlob(i));
                            break;
                        case Types.DOUBLE:
                            obj.put(columnName, resultSet.getDouble(i));
                            break;
                        case Types.FLOAT:
                            obj.put(columnName, resultSet.getFloat(i));
                            break;
                        case Types.INTEGER:
                            obj.put(columnName, resultSet.getInt(i));
                            break;
                        case Types.NVARCHAR:
                            obj.put(columnName, resultSet.getNString(i));
                            break;
                        case Types.VARCHAR:
                            obj.put(columnName, resultSet.getString(i));
                            break;
                        case Types.TINYINT:
                            obj.put(columnName, resultSet.getInt(i));
                            break;
                        case Types.SMALLINT:
                            obj.put(columnName, resultSet.getInt(i));
                            break;
                        case Types.DATE:
                            obj.put(columnName, resultSet.getDate(i));
                            break;
                        case Types.TIMESTAMP:
                            obj.put(columnName, resultSet.getTimestamp(i));
                            break;
                        default:
                            obj.put(columnName, resultSet.getString(i));
                            break;

                    }
                }
            }

            String key = props.getProperty("key");
            String record = obj.toString();

            producer.send(new ProducerRecord<>(topic, key, record), (m, e) -> {
                if (e != null) {
                    LOG.error(e);
                }/*else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                }*/
            });

        }


        producer.flush();
        producer.close();

        return 0;
    }


    @Override
    protected void createStagingTable() {
    }

    @Override
    protected void mergeStagingTable() {/*Not implemented*/}

    @Override
    public void preSourceTasks() {/*Not implemented*/}

    @Override
    public void postSourceTasks() {/*Not implemented*/}


    @Override
    public void postSinkTasks() throws Exception {
        /*Not implemented*/
    }

    @Override
    public void cleanUp() throws Exception {
        /*Not implemented*/
    }


}
