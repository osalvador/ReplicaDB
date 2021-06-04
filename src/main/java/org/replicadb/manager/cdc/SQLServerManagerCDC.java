package org.replicadb.manager.cdc;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.SQLServerManager;

import java.net.URI;
import java.util.List;
import java.util.Properties;

public class SQLServerManagerCDC extends SQLServerManager implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

    private static final Logger LOG = LogManager.getLogger(SQLServerManagerCDC.class);
    private static final String DEBEZIUM_CONNECTOR_CLASS = "io.debezium.connector.sqlserver.SqlServerConnector";

    /**
     * Constructs the SqlManager.
     *
     * @param opts   the ReplicaDB ToolOptions describing the user's requested action.
     * @param dsType the DataSource Type
     */
    public SQLServerManagerCDC(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
    }

    @Override
    public Properties getDebeziumProps() {
        // Define the configuration for the embedded and SQLServer connector ...
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
        //props.setProperty("snapshot.isolation.mode", "read_committed");
        props.setProperty("snapshot.mode","schema_only");
        //props.setProperty("provide.transaction.metadata", "true");
        props.setProperty("tombstones.on.delete", "false"); // only a delete event is sent.
        props.setProperty("converter.schemas.enable", "false"); // don't include schema in message

        props.setProperty("event.processing.failure.handling.mode","fail");

        /* connector properties */
        props.setProperty("connector.class", DEBEZIUM_CONNECTOR_CLASS);
        props.setProperty("database.server.name", "ReplicaDB_SQLServerCDC");
        props.putAll(getDebeziumDatabaseConnection());
        props.setProperty("database.user", this.options.getSourceUser());
        props.setProperty("database.password", this.options.getSourcePassword());

        props.setProperty("table.include.list", this.options.getSourceTable() != null ? this.options.getSourceTable() : "");

        /* File names */
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");

        String fileNamePattern = "ReplicaDB_SQLServerCDC_%s_%s.dat";
        String dbHistoriFileName = String.format(fileNamePattern, props.getProperty("database.dbname"), "dbhistory");
        props.setProperty("database.history.file.filename", "data/" + dbHistoriFileName);

        String offsetFileName = String.format(fileNamePattern, props.getProperty("database.dbname"), "offset");
        props.setProperty("offset.storage.file.filename", "data/" + offsetFileName);

        // Overwrite all defined properties
        //props.setProperty("decimal.handling.mode", "string");
        props.putAll(this.options.getSourceConnectionParams());
        // Transforms
        props.setProperty("transforms", "filter");
        props.setProperty("transforms.filter.type", "org.replicadb.manager.cdc.Filter");
        return props;
    }

    private Properties getDebeziumDatabaseConnection() {
        String urlConnectionString = this.options.getSourceConnect();
        // clean URI from jdbc:sqlserver://172.30.229.207:1433;database=master
        // to sqlserver://172.30.229.207:1433
        // remove jdbc: and ;dbname=master
        String cleanURI = urlConnectionString.substring(5).replaceAll(";.*$", "");
        URI uri = URI.create(cleanURI);
        String hostName = uri.getHost();
        String port = String.valueOf(uri.getPort());

        String dbname = null;
        String[] urlConnectionStringProperties = urlConnectionString.split(";");
        for (int i = 1; i < urlConnectionStringProperties.length; i++) {
            if (urlConnectionStringProperties[i].contains("database")) {
                dbname = urlConnectionStringProperties[i].replaceAll("^.*=", "");
                break;
            }
        }

        LOG.info("Connection params host: {}, port: {}, dbname: {}", hostName, port, dbname);

        Properties props = new Properties();
        props.setProperty("database.hostname", hostName);
        props.setProperty("database.port", port);
        props.setProperty("database.dbname", dbname);
        return props;
    }

    @Override
    public void handleBatch(List<RecordChangeEvent<SourceRecord>> records, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException {
        // Not implemented
    }
}
