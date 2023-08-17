package org.couchbase;


import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;


public class TelematicsDataProducer extends Thread {

    // Load data in queue
    private BlockingQueue<List<JsonObject>> tasksQueue;

    public TelematicsDataProducer(BlockingQueue<List<JsonObject>> tasksQueue) {
        super("PRODUCER");
        this.tasksQueue = tasksQueue;
    }

    public void run() {

        while (true) {
            try {
                List<JsonObject> bulkData = generateMockDataParallel();

                // the producer will add an element into the shared queue.
                tasksQueue.put(bulkData);

                System.out.println("@@@@@@@@@ PRODUCED @@@@@@@@ " + tasksQueue.size());
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

        return Flux.range(1, 10)
                .parallel(10)
                .runOn(Schedulers.parallel())
                .map(i -> generateMockData(i))
                .doOnError(e -> Flux.empty())
                .sequential()
                .collectList()
                .block();
    }

    private static JsonObject generateMockData(int index) {

        Random random = new Random();

        //JsonObject jsonData = JsonObject.create();
        JsonObject jsonData = JsonObject.from(new LinkedHashMap<>());

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
}
