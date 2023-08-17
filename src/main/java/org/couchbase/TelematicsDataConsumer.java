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

				System.out.println(" CONSUMED \n");
				System.out.println(" Thread Name: " + Thread.currentThread().getName());
//				Thread.sleep(2000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private List<MutationResult> bulkInsert(List<JsonObject> data) {
		DatabaseConfiguration dbConfig = DatabaseConfiguration.getInstance();
		ReactiveCluster reactiveCluster = dbConfig.getCluster().reactive();
		ReactiveBucket reactiveBucket = dbConfig.getBucket().reactive();
		ReactiveScope reactiveScope = dbConfig.getScope().reactive();
		ReactiveCollection reactiveCollection = dbConfig.getCollection().reactive();
		int concurrentOps = 100;
		return Flux.fromIterable(data)
				.parallel(concurrentOps)
				.runOn(Schedulers.boundedElastic()) // or one of your choice
				.flatMap(doc -> reactiveCollection.upsert(doc.getString("MessageId"),doc))
		        .doOnError(e -> Flux.empty())
//				.doOnError(e -> Mono.empty())
//                .concatMap(doc -> collection.upsert(doc.getString("key"),doc, UpsertOptions.upsertOptions().durability(finalDlevel)),16)
				.sequential()
				.collectList()
				.block();
	}
}
