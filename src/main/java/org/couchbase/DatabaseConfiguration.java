package org.couchbase;

import com.couchbase.client.java.*;

import java.time.Duration;

public class DatabaseConfiguration {

	public static String CONNECTION_STRING = "couchbases://cb.xsw2nfagx8itqwe.cloud.couchbase.com";
	private static final String USERNAME = "abhijeet";

	private static final String PASSWORD = "Password@P1";

	private static final String BUCKET = "fleetdata";

	private static final String SCOPE = "_default";
	private static final String COLLECTION = "_default";

	public static final Cluster cluster = Cluster.connect(
			CONNECTION_STRING,
			ClusterOptions.clusterOptions(USERNAME, PASSWORD).environment(env -> {
				// Sets a pre-configured profile called "wan-development" to help avoid
				// latency issues when accessing Capella from a different Wide Area Network
				// or Availability Zone (e.g. your laptop).
				env.applyProfile("wan-development");
			})
	);
	public static final Bucket bucket= cluster.bucket(BUCKET);;
	public static final Scope scope =bucket.scope(SCOPE);

	public static final Collection collection=scope.collection(COLLECTION);

	static
	{
		bucket.waitUntilReady(Duration.ofSeconds(10));
	}
}
