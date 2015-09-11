package com.digitalroute.agg;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import com.couchbase.client.CouchbaseClient;

public class CbTest {

	public static void main(String[] args) throws IOException, URISyntaxException {
		final CouchbaseClient client = new CouchbaseClient(Arrays.asList(new URI("http://127.0.0.1:8091/pools")), "aggregation", "");
		System.out.println(client.getAndLock("asdf", 0));

	}

}
