package org.couchbase;

import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This initiates the process of telematics data load generation in Couchbase.
 * 
 * @author abhijeetbehera
 */
public class LaunchTelematicsDataLoader {

	public static void main(String[] args) {

		BlockingQueue<List<JsonObject>> sharedTasksQueue = new LinkedBlockingQueue<List<JsonObject>>();

		ExecutorService executorService = Executors.newFixedThreadPool(64);

		for (int i=0;i<32;i++) {

			// Create number of task producer threads
			executorService.execute(new TelematicsDataProducer(sharedTasksQueue));
			executorService.execute(new TelematicsDataConsumer(sharedTasksQueue));
		}
	}
}
