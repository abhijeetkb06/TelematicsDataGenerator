package org.couchbase;

import com.couchbase.client.java.*;
import java.time.Duration;

public class DatabaseConfiguration {

	private static final String CONNECTION_STRING = "couchbases://cb.xsw2nfagx8itqwe.cloud.couchbase.com";
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
		scope = bucket.scope(SCOPE);
		collection = scope.collection(COLLECTION);

		bucket.waitUntilReady(Duration.ofSeconds(10));
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

	public Cluster getCluster() {
		return cluster;
	}

	public Bucket getBucket() {
		return bucket;
	}

	public Scope getScope() {
		return scope;
	}

	public Collection getCollection() {
		return collection;
	}
}
