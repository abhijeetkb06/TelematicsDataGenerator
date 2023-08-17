package org.example;

//@author -Abhijeet
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryResult;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class CouchbaseTelematicsDataGenerator {
    private static final String CONNECTION_STRING = "couchbases://cb.go19gk8i9yxj4jom.cloud.couchbase.com";
    private static final String USERNAME = "abhijeet";
    private static final String PASSWORD = "Password@P1";
    private static final String BUCKET_NAME = "fleetdata";

    private Cluster cluster;
    private Bucket bucket;
    private Collection collection;

    public CouchbaseTelematicsDataGenerator() {
        cluster = Cluster.connect(CONNECTION_STRING, USERNAME, PASSWORD);
        bucket = cluster.bucket(BUCKET_NAME);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        collection = bucket.defaultCollection();
    }

    public JsonObject create(JsonObject content) {
        String documentId = content.getString("MessageId");
        Collection collection = bucket.defaultCollection();
        collection.insert(documentId, content);
        return read(documentId);
    }

    public JsonObject read(String id) {
        GetResult getResult = collection.get(id);
        return getResult.contentAsObject();
    }

    public QueryResult readBySQL(String statement) {

        QueryResult result = cluster.query(statement);
        return result;
    }

    public JsonObject update(String id, JsonObject content) {
        collection.upsert(id, content);
        return read(id);
    }

    public void delete(String id) {
        collection.remove(id);
    }

    public static void main(String[] args) {

        CouchbaseTelematicsDataGenerator insert = new CouchbaseTelematicsDataGenerator();
        System.out.println("********************* CRUD operations STARTED *****************: ");

        Random random = new Random();

        while (true) {
            // Create a new JsonObject
            JsonObject jsonObject = generateMockData();
            JsonObject created = insert.create(jsonObject);
            String id = created.getString("MessageId");
            System.out.println("Message Created: " + id);
            System.out.println("JSON data in Message: " + jsonObject);
        }
    }

    private static JsonObject generateMockData() {
        Random random = new Random();
        JsonObject jsonData = JsonObject.create();
        jsonData.put("MessageId", UUID.randomUUID());
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
}