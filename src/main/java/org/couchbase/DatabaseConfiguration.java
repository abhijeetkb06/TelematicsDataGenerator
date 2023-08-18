package org.couchbase;

import com.couchbase.client.java.*;

import java.time.Duration;

/**
 * This singleton class ensures only one instance of couchbase cluster connection.
 *
 * @author abhijeetbehera
 */
public class DatabaseConfiguration {

	private static final String CONNECTION_STRING = "couchbases://cb.gbfmqdakjgfavjg.cloud.couchbase.com";
	private static final String USERNAME = "abhijeet";
	private static final String PASSWORD = "Password@P1";
	private static final String BUCKET = "fleetdata";
	private static final String SCOPE = "_default";
	private static final String COLLECTION = "_default";

	private static volatile DatabaseConfiguration instance;

	private static final Cluster cluster;
	private static final Bucket bucket;
	private static final Scope scope;
	private static final Collection collection;

	static {
		cluster = Cluster.connect(
				CONNECTION_STRING,
				ClusterOptions.clusterOptions(USERNAME, PASSWORD).environment(env -> {
					env.applyProfile("wan-development");
				})
		);

		bucket = cluster.bucket(BUCKET);
		bucket.waitUntilReady(Duration.ofSeconds(10));
		scope = bucket.scope(SCOPE);
		collection = scope.collection(COLLECTION);
	}

	private DatabaseConfiguration() {
		// Private constructor to prevent instantiation from outside
	}

	public static DatabaseConfiguration getInstance() {
		if (instance == null) {
			synchronized (DatabaseConfiguration.class) {
				if (instance == null) {
					instance = new DatabaseConfiguration();
				}
			}
		}
		return instance;
	}

	public ReactiveCluster getCluster() {
		return cluster.reactive();
	}

	public ReactiveBucket getBucket() {
		return bucket.reactive();
	}

	public ReactiveScope getScope() {
		return scope.reactive();
	}

	public ReactiveCollection getCollection() {
		return collection.reactive();
	}
}
