package org.replicadb;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.manager.ConnManager;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReplicaDBCDC implements Runnable {
    private static final Logger LOG = LogManager.getLogger(ReplicaDBCDC.class.getName());

    private final ConnManager sourceDs;
    private final ConnManager sinkDs;

    private DebeziumEngine<?> engine;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public ReplicaDBCDC(ConnManager sourceDs, ConnManager sinkDs) {
        this.sourceDs = sourceDs;
        this.sinkDs = sinkDs;
    }

    @Override
    public void run() {
        // Create the engine with this configuration ...
        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(sourceDs.getDebeziumProps())
                .notifying((DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>) sinkDs)
                .build();

        // Run the engine asynchronously ...
        executor.execute(engine);
        LOG.info("Engine executor started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Requesting embedded engine to shut down");
            try {
                engine.close();
                executor.shutdown(); // TODO ???
                awaitTermination(executor);
                LOG.info("Embedded engine is down");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        // the submitted task keeps running, only no more new ones can be added
        executor.shutdown();
        awaitTermination(executor);

    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.debug("Waiting another 10 seconds for the embedded engine to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
