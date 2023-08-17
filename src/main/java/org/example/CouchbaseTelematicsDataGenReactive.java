package org.example;

import com.couchbase.client.java.*;

import com.couchbase.client.java.json.JsonObject;

import com.couchbase.client.java.kv.GetResult;

import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;

import java.util.ArrayList;

import java.util.List;

import java.util.Random;

import java.util.UUID;

public class CouchbaseTelematicsDataGenReactive {

    private static final String CONNECTION_STRING = "couchbases://cb.go19gk8i9yxj4jom.cloud.couchbase.com";

    private static final String USERNAME = "abhijeet";

    private static final String PASSWORD = "Password@P1";

    private static final String BUCKET = "fleetdata";

    private static final String SCOPE = "_default";
    private static final String COLLECTION = "_default";

    private final Cluster cluster;
    private final Collection collection;

    private final Scope scope;

    private final Bucket bucket;


    public CouchbaseTelematicsDataGenReactive() {

//        cluster = ReactiveCluster.connect(CONNECTION_STRING, USERNAME, PASSWORD);

//        cluster = Cluster.connect(CONNECTION_STRING, ClusterOptions.clusterOptions(USERNAME, PASSWORD));

         cluster = Cluster.connect(CONNECTION_STRING, USERNAME, PASSWORD);

         bucket = cluster.bucket(BUCKET);
         bucket.waitUntilReady(Duration.ofSeconds(10));
         scope = bucket.scope(SCOPE);
         collection = scope.collection(COLLECTION);

//        collection.waitUntilReady(Duration.ofSeconds(10)).block();

    }

    public static void main(String[] args) {

        CouchbaseTelematicsDataGenReactive generator = new CouchbaseTelematicsDataGenReactive();

        System.out.println("********************* Reactive operations STARTED *****************: ");

        Random random = new Random();

        List<JsonObject> bulkData = new ArrayList<>();

        while (true) {

            // Generate mock JsonObject

            bulkData = generateMockDataParallel();

//            if (bulkData.size() >= 1000) {

                // Perform bulk inserts in parallel

                List<MutationResult> createdDocuments = generator.bulkInsert(bulkData);

              /*  for (MutationResult document : createdDocuments) {

                *//*    String id = document.getString("MessageId");

                    System.out.println("Message Created: " + id);*//*

                }*/

                bulkData.clear();
//            }

        }

    }

    public static List<JsonObject> generateMockDataParallel() {

        return Flux.range(1, 10)
                .parallel(10)
                .runOn(Schedulers.parallel())
                .map(i -> generateMockData(i))
                .sequential()
                .collectList()
                .block();
    }

    private static JsonObject generateMockData(int index) {

        Random random = new Random();

        JsonObject jsonData = JsonObject.create();

        jsonData.put("MessageId", UUID.randomUUID().toString());

        jsonData.put("DeviceId", "vehicle" + random.nextInt(100));

        jsonData.put("EventTime", Instant.now().toString());

        jsonData.put("Orgs", JsonObject.create().put("org1", random.nextInt(100)).put("org2", random.nextInt(100)));

        JsonObject payload = JsonObject.create();

        payload.put("telematics", JsonObject.create()

                .put("vehicleId", "ABC" + random.nextInt(1000))

                .put("location", JsonObject.create()

                        .put("latitude", 30 + random.nextDouble() * 20)

                        .put("longitude", -120 + random.nextDouble() * 60))

                .put("speed", random.nextInt(100))

                .put("fuelLevel", random.nextInt(100))

                .put("engineStatus", random.nextBoolean() ? "running" : "stopped")

                .put("tirePressure", JsonObject.create()

                        .put("frontLeft", 28 + random.nextInt(10))

                        .put("frontRight", 28 + random.nextInt(10))

                        .put("rearLeft", 28 + random.nextInt(10))

                        .put("rearRight", 28 + random.nextInt(10)))

                .put("driver", JsonObject.create()

                        .put("name", "Driver" + random.nextInt(1000))

                        .put("licenseNumber", "ABC" + random.nextInt(1000))

                        .put("status", random.nextBoolean() ? "active" : "inactive")));

        jsonData.put("Payload", payload);

        return jsonData;

    }

    private List<MutationResult> bulkInsert(List<JsonObject> data) {

        ReactiveCluster reactiveCluster = cluster.reactive();
        ReactiveBucket reactiveBucket = bucket.reactive();
        ReactiveScope reactiveScope = scope.reactive();
        ReactiveCollection reactiveCollection = collection.reactive();
        int concurrentOps = 1;
        return Flux.fromIterable(data)
                .parallel(concurrentOps)
                .runOn(Schedulers.boundedElastic()) // or one of your choice
                .flatMap(doc -> reactiveCollection.upsert(doc.getString("MessageId"),doc))
//                .concatMap(doc -> collection.upsert(doc.getString("key"),doc, UpsertOptions.upsertOptions().durability(finalDlevel)),16)
                .sequential()
                .collectList()
                .block();

/*         Flux.fromIterable(data)

                .flatMap(document -> collection.insert(document.getString("MessageId"), document))

                .collectList()

                .block();*/
    }
}