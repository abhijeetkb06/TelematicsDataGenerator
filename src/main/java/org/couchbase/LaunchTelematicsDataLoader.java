package org.couchbase;

import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

/**
 * This initiates the process of telematics data load generation in Couchbase.
 * 
 * @author abhijeetbehera
 */
public class LaunchTelematicsDataLoader {

	public static void main(String[] args) {

		BlockingQueue<List<JsonObject>> sharedTasksQueue = new LinkedBlockingQueue<List<JsonObject>>();

		ExecutorService executorService = Executors.newFixedThreadPool(100);

		IntStream.range(0, 10)
				.forEach(i -> {
					executorService.execute(new TelematicsDataProducer(sharedTasksQueue));
				});

		IntStream.range(0, 10)
				.forEach(i -> {
					executorService.execute(new TelematicsDataConsumer(sharedTasksQueue));
				});
	}
}
