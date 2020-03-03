package org.replicadb.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

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

        Properties kafkaProps = options.getSinkConnectionParams();
        String topic = kafkaProps.getProperty("topic");
        Integer partition = kafkaProps.getProperty("partition") != null ? Integer.parseInt(kafkaProps.getProperty("partition")) : null;
        String key = kafkaProps.getProperty("key");

        final Properties props = options.getSinkConnectionParams();
        // Default Kafka properties.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getSinkConnect().substring("kafka://".length()));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // User Custom kafka properties
        props.putAll(kafkaProps);

        LOG.debug("kafka properties: " + props);

        // Create kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create array form sink column names
        String[] sinkColumns = getAllSinkColumns(rsmd).split(",");

        // Date Conversion
        SimpleDateFormat dateFormat;
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        SimpleDateFormat timestampFormat;
        timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        // JSON objects
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode obj;


        if (resultSet.next()) {
            // Create Bandwidth Throttling
            bandwidthThrottlingCreate(resultSet, rsmd);

            do {
                bandwidthThrottlingAcquiere();

                // Just one sink column and is a JSON.
                // Create a JSON object form content.
                if (sinkColumns[0].toLowerCase().equals("json")) {
                    obj = (ObjectNode) mapper.readTree(resultSet.getString(1));
                } else {

                    obj = mapper.createObjectNode();

                    for (int i = 1; i < columnsNumber + 1; i++) {

                        String columnName = sinkColumns[i - 1];

                        switch (rsmd.getColumnType(i)) {
                            case Types.VARCHAR:
                            case Types.CHAR:
                            case Types.LONGVARCHAR:
                                obj.put(columnName, resultSet.getString(i));
                                break;
                            case Types.BIGINT:
                            case Types.INTEGER:
                            case Types.TINYINT:
                            case Types.SMALLINT:
                                obj.put(columnName, resultSet.getInt(i));
                                break;
                            case Types.NUMERIC:
                            case Types.DECIMAL:
                                obj.put(columnName, resultSet.getBigDecimal(i));
                                break;
                            case Types.DOUBLE:
                                obj.put(columnName, resultSet.getDouble(i));
                                break;
                            case Types.FLOAT:
                                obj.put(columnName, resultSet.getFloat(i));
                                break;
                            case Types.DATE:
                                obj.put(columnName, dateFormat.format(resultSet.getDate(i).getTime()));
                                break;
                            case Types.TIMESTAMP:
                            case Types.TIMESTAMP_WITH_TIMEZONE:
                            case -101:
                            case -102:
                                Timestamp timeStamp = resultSet.getTimestamp(i);
                                if (timeStamp != null)
                                    obj.put(columnName, timestampFormat.format(resultSet.getTimestamp(i).getTime()));
                                else
                                    obj.put(columnName,"");
                                break;
                            case Types.BINARY:
                            case Types.BLOB:
                                // Binary data encode to Base64
                                Blob data = resultSet.getBlob(i);
                                obj.put(columnName, data.getBytes(1, (int) data.length()));
                                data.free();
                                break;
                            case Types.CLOB:
                                obj.put(columnName, clobToString(resultSet.getClob(i)));
                                break;
                            case Types.BOOLEAN:
                                obj.put(columnName, resultSet.getBoolean(i));
                                break;
                            case Types.NVARCHAR:
                                obj.put(columnName, resultSet.getNString(i));
                                break;
                            case Types.SQLXML:
                                obj.put(columnName, sqlxmlToString(resultSet.getSQLXML(i)));
                                break;
                        /*case Types.ROWID:
                            obj.put(columnName, resultSet.getRowId(i));
                            break;
                        case Types.ARRAY:
                            obj.put(columnName, resultSet.getArray(i));
                            break;*/
                            default:
                                obj.put(columnName, resultSet.getString(i));
                                break;

                        }
                    }
                }

                // Send to kafka
                producer.send(new ProducerRecord<>(topic, partition, key, mapper.writeValueAsString(obj)), (m, e) -> {
                    if (e != null) {
                        LOG.error(e);
                    }/*else {
                      System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                    }*/
                });
            } while (resultSet.next());
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
    public void postSinkTasks() {/*Not implemented*/}

    @Override
    public void cleanUp() {/*Not implemented*/}


}
