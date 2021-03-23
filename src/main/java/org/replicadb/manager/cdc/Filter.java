package org.replicadb.manager.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class Filter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LogManager.getLogger(Filter.class);
    private final HashMap<String, String> conditions = new HashMap<>();

    @Override
    public R apply(R record) {
        // Apply filter
        if (record != null && !conditions.isEmpty())
        {
            String tableName = getSourceTableName(record);
            Envelope.Operation operation = Envelope.operationFor((SourceRecord) record);

            if (conditions.get(tableName) != null && operation != null && conditions.get(tableName).contains(operation.code()) ){
                LOG.debug("Filter applied. Source table: {} operation {} ", tableName, operation.code());
                return null;
            }
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = (String) map.get("condition");

            if (json == null){
                LOG.debug("No filtering conditions have been defined");
                return;
            };

            HashMap<String,String>[] jsonMap = mapper.readValue(json, HashMap[].class);

            for (HashMap<String,String> item : jsonMap) {
                conditions.put( item.get("table"), item.get("operations"));
            }

            LOG.debug("Defined filtering conditions:{}",conditions);

        } catch (JsonProcessingException e) {
            LOG.error(e);
            close();
        }
    }

    private String getSourceTableName(R recordValue) {
        Struct struct = (Struct) recordValue.value();
        String table =struct.getStruct("source").getString("table");
        String schema = struct.getStruct("source").getString("schema");
        // get source
        return schema + "." + table;
    }
}
