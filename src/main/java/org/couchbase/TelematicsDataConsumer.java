package org.couchbase;

import com.couchbase.client.java.ReactiveBucket;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.couchbase.TelematicsDataProducer.generateMockDataParallel;

public class TelematicsDataConsumer extends Thread {

	// Read data to consume once data is loaded in queue
	private BlockingQueue<List<JsonObject>> tasksQueue;

	public TelematicsDataConsumer(BlockingQueue<List<JsonObject>> tasksQueue) {
		super("CONSUMER");
		this.tasksQueue = tasksQueue;
	}

	public void run() {
		try {
			while (true) {
				
				System.out.println("***************QUEUE SIZE************** "+ tasksQueue.size());

				// Remove the user from shared queue and process
				bulkInsert(tasksQueue.take());

//				bulkInsert(generateMockDataParallel());
				System.out.println(" CONSUMED \n");
				System.out.println(" Thread Name: " + Thread.currentThread().getName());
//				Thread.sleep(2000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private List<MutationResult> bulkInsert(List<JsonObject> data) {
		DatabaseConfiguration dbConfig = DatabaseConfiguration.getInstance();
		int concurrentOps = 20;
		return Flux.fromIterable(data)
				.parallel(concurrentOps)
				.runOn(Schedulers.boundedElastic()) // or one of your choice
				.flatMap(doc -> dbConfig.getCollection().upsert(doc.getString("MessageId"),doc))
		        .doOnError(e -> Flux.empty())
//                .concatMap(doc -> collection.upsert(doc.getString("key"),doc, UpsertOptions.upsertOptions().durability(finalDlevel)),16)
				.sequential()
				.collectList()
				.block();
	}
}
