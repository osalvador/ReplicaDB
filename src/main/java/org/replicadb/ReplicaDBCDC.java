package org.replicadb;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.manager.ConnManager;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReplicaDBCDC implements Runnable {
    private static final Logger LOG = LogManager.getLogger(ReplicaDBCDC.class.getName());

    public final ConnManager sourceDs;
    public final ConnManager sinkDs;

    private DebeziumEngine<?> engine;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public ReplicaDBCDC(ConnManager sourceDs, ConnManager sinkDs) {
        this.sourceDs = sourceDs;
        this.sinkDs = sinkDs;
    }

    @Override
    public void run() {
        Properties debeziumProps = sourceDs.getDebeziumProps();

        // Create the engine with this configuration ...
        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(debeziumProps)
                .notifying((DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>) sinkDs)
                .using(new EngineCompletionCallBack())
                .build();

        // Run the engine asynchronously ...
        executor.execute(engine);

        LOG.info("Engine executor started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Requesting embedded engine to shut down");
            try {
                engine.close();
                awaitTermination(executor);
                LOG.info("Embedded engine is down");
            } catch (Exception e) {
                LOG.error("Error stopping Embedded engine: {}", e.getMessage());
                LOG.error("Salgo por aqui");
                LOG.error(e);
            }
        }));

        // the submitted task keeps running, only no more new ones can be added
        executor.shutdown();
        awaitTermination(executor);

    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.info("Waiting another 10 seconds for the embedded engine to complete");

               /* try {
                    // TODO if streaming es false me piro
                    LOG.info("Me piro");
                    engine.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.error("Salgo por aqui");
                }
                executor.shutdown();

                */

            }
        } catch (Exception e) {
            LOG.error("Salgo por aqui");
            Thread.currentThread().interrupt();
        }
    }

    public static class EngineCompletionCallBack implements DebeziumEngine.CompletionCallback {
        @Override
        public void handle(boolean success, String message, Throwable error) {
            LOG.error("por aqui: {} ",success);
            //LOG.error("por aqui: {} ",message);
            LOG.error(error);

            if (!success && message.contains("Unable to initialize")){
                // rearrancar el engine TODO
                LOG.error("Rearrancar la aplicación aqui! no se como...");
            }

        }
    }
}
