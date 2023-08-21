package org.couchbase;


import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * This initiates the process of bulk mock data generation in Couchbase using reactive apis.
 *
 * @author abhijeetbehera
 */
public class TelematicsDataProducer extends Thread {

    // Load data in queue
    private BlockingQueue<List<JsonObject>> sharedQueue;

    public TelematicsDataProducer(BlockingQueue<List<JsonObject>> sharedQueue) {
        super("PRODUCER");
        this.sharedQueue = sharedQueue;
    }

    public void run() {

        while (true) {
            try {
                List<JsonObject> mockDataList = generateMockDataParallel();

                // Add list of mock data to shared queue.
                sharedQueue.put(mockDataList);

                System.out.println("@@@@@@@@@ PRODUCED @@@@@@@@ " + sharedQueue.size());
                System.out.println(" Thread Name: " + Thread.currentThread().getName());

                Thread.sleep(50);
            } catch (InterruptedException e) {
            } catch (CouchbaseException ex) {
                System.err.println("Something else happened: " + ex);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static List<JsonObject> generateMockDataParallel() {

        return Flux.range(ConcurrencyConfig.MOCK_DATA_START_RANGE, ConcurrencyConfig.MOCK_DATA_END_RANGE)
                .parallel(ConcurrencyConfig.MOCK_DATA_PARALLELISM)
                .runOn(Schedulers.parallel())
                .map(i -> generateMockData(i))
                .doOnError(e -> Flux.empty())
                .sequential()
                .collectList()
                .block();
    }

    private static JsonObject generateMockData(int index) {
        Random random = new Random();

        JsonObject jsonData = JsonObject.from(new LinkedHashMap<>());

        jsonData.put("MessageId", UUID.randomUUID().toString());
        jsonData.put("DeviceId", "vehicle" + random.nextInt(100));
        jsonData.put("EventTime", Instant.now().toString());

        // Orgs represented as an array of objects
        JsonArray orgsArray = JsonArray.from("org1" + random.nextInt(1000), "org2" + random.nextInt(1000));
        jsonData.put("Orgs", orgsArray);

        JsonObject payload = JsonObject.from(new LinkedHashMap<>());

        JsonObject telematics = JsonObject.from(new LinkedHashMap<>());
        telematics.put("vehicleId", "ABC" + random.nextInt(1000));

        JsonObject location = JsonObject.from(new LinkedHashMap<>());
        location.put("latitude", 30 + random.nextDouble() * 20);
        location.put("longitude", -120 + random.nextDouble() * 60);
        telematics.put("location", location);

        telematics.put("speed", random.nextInt(100));
        telematics.put("fuelLevel", random.nextInt(100));
        telematics.put("engineStatus", random.nextBoolean() ? "running" : "stopped");

        JsonObject tirePressure = JsonObject.from(new LinkedHashMap<>());
        tirePressure.put("frontLeft", 28 + random.nextInt(10));
        tirePressure.put("frontRight", 28 + random.nextInt(10));
        tirePressure.put("rearLeft", 28 + random.nextInt(10));
        tirePressure.put("rearRight", 28 + random.nextInt(10));
        telematics.put("tirePressure", tirePressure);

        JsonObject driver = JsonObject.from(new LinkedHashMap<>());
        driver.put("name", "Driver" + random.nextInt(1000));
        driver.put("licenseNumber", "ABC" + random.nextInt(1000));
        driver.put("status", random.nextBoolean() ? "active" : "inactive");
        telematics.put("driver", driver);
        payload.put("telematics", telematics);
        jsonData.put("Payload", payload);
        return jsonData;
    }
}
