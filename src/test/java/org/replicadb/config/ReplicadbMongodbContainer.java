package org.replicadb.config;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ReplicadbMongodbContainer extends MongoDBContainer {
   private static final Logger LOG = LogManager.getLogger(ReplicadbMongodbContainer.class);
   private static final String IMAGE_VERSION = "mongo:4.2.23";
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String SOURCE_FILE = "/mongo/t_source_data.json";
   private static final String T_SOURCE_COLLECTION = "t_source";
   private static ReplicadbMongodbContainer container;
   private ConnectionString connectionString;

   private ReplicadbMongodbContainer () {
      super(IMAGE_VERSION);
   }

   public static ReplicadbMongodbContainer getInstance () {
      if (container == null) {
         container = new ReplicadbMongodbContainer();
         container.start();
      }
      return container;
   }

   @Override
   public void start () {
      super.start();
      connectionString = new ConnectionString(container.getReplicaSetUrl());

      String uri = container.getReplicaSetUrl();
      try (MongoClient mongoClient = MongoClients.create(uri)) {
         MongoDatabase database = mongoClient.getDatabase(connectionString.getDatabase());
         try {
            List<String> allLines = Files.readAllLines(Paths.get(RESOURCE_DIR + SOURCE_FILE));

            for (String line : allLines) {
               // if line is empty or a comment, skip it
               if (line.trim().isEmpty() || line.trim().startsWith("//")) {
                  continue;
               }
               Document doc = Document.parse(line);
               database.getCollection(T_SOURCE_COLLECTION).insertOne(doc);
            }
            // count inserted documents
            long count = database.getCollection(T_SOURCE_COLLECTION).countDocuments();
            LOG.info("Inserted " + count + " documents into the '" + T_SOURCE_COLLECTION + "' collection");
            // log the first document
            LOG.info("First document: " + database.getCollection(T_SOURCE_COLLECTION).find().first().toJson());

            database.createCollection("t_sink");
            // create unique index on the sink collection on field C_INTEGER
            database.getCollection("t_sink").createIndex(new Document("c_integer", 1), new IndexOptions().unique(true));

         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }


   @Override
   public void stop () {
      //do nothing, JVM handles shut down
   }

   public ConnectionString getMongoConnectionString () {
      return connectionString;
   }
}
