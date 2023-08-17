package org.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

import java.time.Duration;

public class DatabaseConfiguration {

	public static String CONNECTION_STRING = "couchbases://cb.go19gk8i9yxj4jom.cloud.couchbase.com";
	private static final String USERNAME = "abhijeet";

	private static final String PASSWORD = "Password@P1";

	private static final String BUCKET = "fleetdata";

	private static final String SCOPE = "_default";
	private static final String COLLECTION = "_default";

	public static final Cluster cluster = Cluster.connect(CONNECTION_STRING, USERNAME, PASSWORD);
	public static final Bucket bucket= cluster.bucket(BUCKET);;
	public static final Scope scope =bucket.scope(SCOPE);

	public static final Collection collection=scope.collection(COLLECTION);

	static
	{
		bucket.waitUntilReady(Duration.ofSeconds(10));
	}
}
